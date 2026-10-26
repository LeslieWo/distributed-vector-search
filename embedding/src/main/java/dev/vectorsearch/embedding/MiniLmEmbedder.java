package dev.vectorsearch.embedding;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * all-MiniLM-L6-v2 on onnxruntime: BERT word-piece tokenize -> transformer -> mean pooling over
 * real (non-padding) tokens -> L2 normalize. Same recipe as the sentence-transformers model card.
 * Thread-safe: OrtSession.run and the tokenizer may be called concurrently.
 */
public final class MiniLmEmbedder implements Embedder {
    public static final int DIMENSION = 384;
    /** sentence-transformers' max_seq_length for this model (tokenizer.json itself says 128). */
    public static final int DEFAULT_MAX_TOKENS = 256;
    public static final int DEFAULT_MICRO_BATCH = 8;

    private final OrtEnvironment env;
    private final OrtSession session;
    private final HuggingFaceTokenizer tokenizer;
    private final boolean needsTokenTypeIds;
    private final int microBatch;

    public MiniLmEmbedder(Path modelDir) {
        this(modelDir, DEFAULT_MAX_TOKENS, DEFAULT_MICRO_BATCH, 0);
    }

    /**
     * @param microBatch max rows per onnxruntime call; longer input lists are split after sorting by length
     * @param intraOpThreads onnxruntime threads per inference; 0 = onnxruntime default (all cores)
     */
    public MiniLmEmbedder(Path modelDir, int maxTokens, int microBatch, int intraOpThreads) {
        if (microBatch <= 0) throw new IllegalArgumentException("microBatch must be > 0");
        this.microBatch = microBatch;
        Path model = modelDir.resolve("model.onnx");
        Path tok = modelDir.resolve("tokenizer.json");
        if (!Files.isRegularFile(model) || !Files.isRegularFile(tok)) {
            throw new IllegalStateException("model files missing in " + modelDir + "; run scripts/fetch-data.sh");
        }
        try {
            // Override the fixed 128 padding baked into tokenizer.json: we pad per batch ourselves.
            tokenizer = HuggingFaceTokenizer.newInstance(tok, Map.of(
                    "padding", "false",
                    "truncation", "true",
                    "maxLength", String.valueOf(maxTokens)));
            env = OrtEnvironment.getEnvironment();
            OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            if (intraOpThreads > 0) opts.setIntraOpNumThreads(intraOpThreads);
            session = env.createSession(model.toString(), opts);
            needsTokenTypeIds = session.getInputNames().contains("token_type_ids");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (OrtException e) {
            throw new IllegalStateException("cannot load ONNX model " + model, e);
        }
    }

    @Override
    public int dimension() {
        return DIMENSION;
    }

    @Override
    public float[][] embed(List<String> texts) {
        if (texts.isEmpty()) return new float[0][];
        Encoding[] enc = tokenizer.batchEncode(texts);
        // Sort by token length and run micro-batches of similar length. A batch is padded to its
        // longest row and attention cost grows with length^2, so mixing a 20-token and a 200-token
        // text wastes most of the work on padding (measured: micro-batches of 8 after sorting ran ~2x faster than one padded call of 64, BUILDLOG step 13).
        Integer[] order = new Integer[enc.length];
        for (int i = 0; i < order.length; i++) order[i] = i;
        Arrays.sort(order, Comparator.comparingInt(i -> enc[i].getIds().length));
        float[][] out = new float[enc.length][];
        for (int from = 0; from < order.length; from += microBatch) {
            int to = Math.min(from + microBatch, order.length);
            Encoding[] group = new Encoding[to - from];
            for (int j = from; j < to; j++) group[j - from] = enc[order[j]];
            float[][] vecs = infer(group);
            for (int j = from; j < to; j++) out[order[j]] = vecs[j - from];
        }
        return out;
    }

    private float[][] infer(Encoding[] enc) {
        int batch = enc.length;
        int seq = 0;
        for (Encoding e : enc) seq = Math.max(seq, e.getIds().length);

        // Right-pad every row to the longest row in this batch; padding has attention_mask = 0.
        long[] ids = new long[batch * seq];
        long[] mask = new long[batch * seq];
        long[] types = new long[batch * seq];
        for (int b = 0; b < batch; b++) {
            long[] rowIds = enc[b].getIds();
            long[] rowMask = enc[b].getAttentionMask();
            long[] rowTypes = enc[b].getTypeIds();
            System.arraycopy(rowIds, 0, ids, b * seq, rowIds.length);
            System.arraycopy(rowMask, 0, mask, b * seq, rowMask.length);
            System.arraycopy(rowTypes, 0, types, b * seq, rowTypes.length);
        }

        long[] shape = {batch, seq};
        Map<String, OnnxTensor> inputs = new HashMap<>();
        try {
            inputs.put("input_ids", OnnxTensor.createTensor(env, LongBuffer.wrap(ids), shape));
            inputs.put("attention_mask", OnnxTensor.createTensor(env, LongBuffer.wrap(mask), shape));
            if (needsTokenTypeIds) inputs.put("token_type_ids", OnnxTensor.createTensor(env, LongBuffer.wrap(types), shape));
            try (OrtSession.Result result = session.run(inputs)) {
                // last_hidden_state: [batch, seq, 384], read as one flat buffer instead of getValue(),
                // which would allocate batch*seq small Java arrays.
                FloatBuffer hidden = ((OnnxTensor) result.get(0)).getFloatBuffer();
                return meanPoolAndNormalize(hidden, mask, batch, seq, DIMENSION);
            }
        } catch (OrtException e) {
            throw new IllegalStateException("onnx inference failed", e);
        } finally {
            inputs.values().forEach(OnnxTensor::close);
        }
    }

    static float[][] meanPoolAndNormalize(FloatBuffer hidden, long[] mask, int batch, int seq, int dim) {
        float[][] out = new float[batch][dim];
        for (int b = 0; b < batch; b++) {
            int count = 0;
            for (int t = 0; t < seq; t++) {
                if (mask[b * seq + t] == 0) continue;
                count++;
                int base = (b * seq + t) * dim;
                for (int d = 0; d < dim; d++) out[b][d] += hidden.get(base + d);
            }
            if (count > 0) for (int d = 0; d < dim; d++) out[b][d] /= count;
            Vectors.normalize(out[b]);
        }
        return out;
    }

    /** Word-piece count including [CLS]/[SEP], after truncation. Used to check chunk sizing. */
    public int tokenCount(String text) {
        return tokenizer.encode(text).getIds().length;
    }

    @Override
    public void close() {
        try {
            session.close();
        } catch (OrtException e) {
            throw new IllegalStateException(e);
        }
        tokenizer.close();
    }
}
