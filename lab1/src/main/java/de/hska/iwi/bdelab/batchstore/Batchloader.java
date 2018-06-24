package de.hska.iwi.bdelab.batchstore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import com.backtype.hadoop.pail.Pail;
import de.hska.iwi.bdelab.schema.*;
import manning.tap.DataPailStructure;
import org.apache.hadoop.fs.FileSystem;

public class Batchloader {
    private long nonce = 0;
    private Pail.TypedRecordOutputStream stream;
    private DataPailStructure pailStructure = new DataPailStructure();

    private void readPageviewsAsStream() {
        try {
            URI uri = Batchloader.class.getClassLoader().getResource("pageviews.txt").toURI();
            try (Stream<String> stream = Files.lines(Paths.get(uri))) {
                stream.forEach(line -> writeToPail(getDatafromString(line)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException e1) {
            e1.printStackTrace();
        }
    }

    private Data getDatafromString(String pageview) {
        StringTokenizer tokenizer = new StringTokenizer(pageview);
        String ip = tokenizer.nextToken();
        String url = tokenizer.nextToken();
        String time = tokenizer.nextToken();

        System.out.println(ip + " " + url + " " + time);

        UserID userId = new UserID();
        userId.set_user_id(ip);

        PageID pageId = new PageID();
        pageId.set_url(url);

        DataUnit du = new DataUnit();
        du.set_pageView(new PageViewEdge(userId, pageId, nonce++));

        return new Data(new Pedigree(Integer.parseInt(time)), du);
    }

    private void writeToPail(Data data) {
        try {
            stream.writeObject(data);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void importPageviews() {

        // change this to "true" if you want to work
        // on the local machines' file system instead of hdfs
        boolean LOCAL = false;

        try {
            // set up filesystem
            FileSystem fs = FileUtils.getFs(LOCAL);

            // prepare temporary pail folder
            String newPath = FileUtils.prepareNewFactsPath(true, LOCAL);

            // master pail goes to permanent fact store
            String masterPath = FileUtils.prepareMasterFactsPath(false, LOCAL);

            // set up new pail and a stream
            Pail p = Pail.create(fs, newPath, pailStructure);
            stream = p.openWrite();

            // write facts to new pail
            readPageviewsAsStream();

            stream.close();

            // set up master pail and absorb new pail
            Pail mp = Pail.create(fs, masterPath, pailStructure);
            mp.absorb(p);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Batchloader loader = new Batchloader();
        loader.importPageviews();
    }
}