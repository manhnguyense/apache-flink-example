package com.se.flink.se.flink.handle;

import com.se.flink.se.flink.domain.CardVM;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;


public class Handle {

    private static final String AES_CBC_MODE = "AES/CBC/PKCS5Padding";
    private static final String AES_NAME = "AES";

    public static void generateCard() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
//                .getExecutionEnvironment();
                .createRemoteEnvironment("localhost", 8081, "target/flink-demon-v1.0.0.jar");
        env.setParallelism(2);
        String initPhone = "0987649607";
        String holderName = "Nguuyen Van A";
        long tokenNo = 9704546218184457L;
        DataStream<Long> myInts = env.fromSequence(0, 400000);
        final SingleOutputStreamOperator<CardVM> map = myInts.map((MapFunction<Long, CardVM>) i -> {
            final CardVM cardVM = new CardVM();
            cardVM.setPhone(initPhone);
            cardVM.setHolderName(holderName + i);
            cardVM.setTokenNo(String.valueOf(tokenNo + 1));
            return cardVM;
        });
        map.addSink(new CsvSinkFunction<>("token1.csv"));
        env.execute();
    }


    public static String encryptToBase64(String keyInStr, String dataToEncrypt) throws Exception {

        keyInStr = formatKey(keyInStr);
        String vectorInStr = getVector(keyInStr);
        byte[] keyInBinary = keyInStr.getBytes();
        byte[] vectorInBinary = vectorInStr.getBytes();

        byte[] encryptedData = encrypt(keyInBinary, vectorInBinary, dataToEncrypt);

        return toBase64RFC(encryptedData);
    }


    public static void handleCsv(Path dataDirectory, String key) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        defineWorkflow(env, dataDirectory, key);
        env.execute();
    }

    public static void defineWorkflow(
            StreamExecutionEnvironment env,
            Path dataDirectory,
            String key) {
        CsvReaderFormat<CardVM> csvFormat = CsvReaderFormat.forPojo(CardVM.class);
        FileSource<CardVM> source =
                FileSource.forRecordStreamFormat(csvFormat, dataDirectory)
                        .build();
        final DataStreamSource<CardVM> file =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "File");
        final SingleOutputStreamOperator<CardVM> map = file.map((MapFunction<CardVM, CardVM>) cardVM -> {
            final String cardNumber = decryptFromBase64(key, cardVM.getCardMasking());
            cardVM.setCardNumber(cardNumber);
            return cardVM;
        });
        map.addSink(new CsvSinkFunction<>("output.csv"));
    }

    public static class CsvSinkFunction<T extends CardVM> extends RichSinkFunction<T> {
        private String filePath;
        private BufferedWriter writer;

        public CsvSinkFunction(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            writer = new BufferedWriter(new FileWriter(filePath));
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            writer.write(value.toString());
            writer.newLine();
        }

        @Override
        public void close() throws Exception {
            super.close();
            writer.close();
        }
    }

    private static String formatKey(String key) {
        if (key == null) {
            return "";
        }

        if (key.length() >= 32) {
            return key.substring(0, 32);
        }

        return key;
    }

    public static String decryptFromBase64(String keyInStr, String cipherText) {

        try {
            keyInStr = formatKey(keyInStr);
            String vectorInStr = getVector(keyInStr);
            byte[] keyInBinary = keyInStr.getBytes();
            byte[] vectorInBinary = vectorInStr.getBytes();

            byte[] dataToDecrypt = fromBase64RFC(cipherText);
            byte[] decrypted = decrypt(keyInBinary, vectorInBinary, dataToDecrypt);

            return new String(decrypted, StandardCharsets.UTF_8);
        } catch (Exception e) {

            System.out.println("maskingCard: " + cipherText + ", Exception: " + e.getMessage());
            return StringUtils.EMPTY;
        }

    }

    private static String toBase64RFC(byte[] data) {
        return Base64.encodeBase64URLSafeString(data).replace('/', '_').replace('+', '-');
    }

    private static byte[] fromBase64RFC(String sBase64) {
        sBase64 = sBase64.replace('_', '/').replace('-', '+');
        return Base64.decodeBase64(sBase64);
    }

    private static String getVector(String key) {
        String vector = key;

        if (key == null) {
            return "";
        }
        if (key.length() >= 16) {
            vector = key.substring(key.length() - 16);
        }

        return vector;
    }

    private static byte[] encrypt(byte[] keyInBinary, byte[] vectorInBinary, String dataToEncrypt) throws Exception {

        SecretKeySpec secretKeySpec = new SecretKeySpec(keyInBinary, AES_NAME);
        IvParameterSpec ivspec = new IvParameterSpec(vectorInBinary);

        Cipher c = Cipher.getInstance(AES_CBC_MODE);
        c.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivspec);

        return c.doFinal(dataToEncrypt.getBytes(StandardCharsets.UTF_8));

    }

    private static byte[] decrypt(byte[] keyInBinary, byte[] vectorInBinary, byte[] dataToDecrypt) throws Exception {

        SecretKeySpec secretKeySpec = new SecretKeySpec(keyInBinary, AES_NAME);
        IvParameterSpec ivspec = new IvParameterSpec(vectorInBinary);

        Cipher c = Cipher.getInstance(AES_CBC_MODE);
        c.init(Cipher.DECRYPT_MODE, secretKeySpec, ivspec);

        return c.doFinal(dataToDecrypt);
    }


}
