package ru.innopolis.dmd.project.innodb.test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

import static java.text.MessageFormat.format;

/**
 * @author Timur Kasatkin
 * @date 16.10.15.
 * @email aronwest001@gmail.com
 */
public class IOSandbox {

    public static void main(String[] args) {
        byte charSize = Character.SIZE / 8;
        File studFile = new File("Student.txt");
        RandomAccessFile raf = null;
        try {
            Files.deleteIfExists(studFile.toPath());
            raf = new RandomAccessFile(studFile, "rw");
            for (int i = 1; i <= 100; i++) {
                String s = format("{0},name {0},email #{0},address #{0}                       \n", i);
                raf.writeChars(s);
//                raf.seek(raf.getFilePointer() - s.length() * charSize);
//                byte[] bytes = new byte[2*s.length()];
//                raf.readFully(bytes);
//                System.out.println(new String(bytes));
//                System.out.println(raf.readLine());
//                raf.skipBytes(s.length() * charSize);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(raf);
        }
        try {
            raf = new RandomAccessFile(studFile, "rw");
            raf.seek(2);
            System.out.println(raf.readChar());
//            while (fp(raf) != raf.length()) {
//                char x = raf.readChar();
//                if (x == '\n') {
//                    raf.seek(fp(raf) - charSize);
//                    raf.writeChar('Y');
//                }
//                System.out.println(x);
//            }
//            for (int i = 1; i <= 100; i++) {
//                String s = raf.readLine();
//                if (i % 2 == 0) {
//                    raf.seek(raf.getFilePointer() -
//                            (s.length() - s.trim().length()) * charSize);
//                    raf.writeChars("$THIS IS STRING!");
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(raf);
        }
    }

    private static long fp(RandomAccessFile raf) throws IOException {
        return raf.getFilePointer();
    }

    private static void close(RandomAccessFile raf) {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static int hash(String str) {
        int hash = 0, i = 5, j = 3;
        for (char c : str.toCharArray()) {
            hash = (hash * i) + c;
            i *= j;
        }
        return hash;
    }

}
