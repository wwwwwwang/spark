package etlutil;


import java.io.*;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

public class PropertiesReader {
    private static final String DEFAULT_ENCODING = "UTF-8";
    private Properties props = new Properties();

    public Properties loadProperties(String fileName) throws IOException {
        String userPath = System.getProperty("user.dir");
        String proPath = userPath + "/" + fileName;
        InputStream proFile = new FileInputStream(proPath);
        //File proFile = new File(proPath);
        //if (!proFile.exists()) {
        if (proFile.available() <= 0 ) {
            throw new RuntimeException("Property file not found: " + proPath);
        }
        //Reader r = new InputStreamReader(new FileInputStream(proFile), DEFAULT_ENCODING);
        props.load(proFile);
        return props;
    }

    public Properties loadPropertiesByClassPath(String fileName) throws IOException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(fileName);

        String proPath = url.getFile();
        proPath = proPath.replaceAll("%20", " ");
        File proFile = new File(proPath);
        if (!proFile.exists()) {
            throw new RuntimeException("Property file not found: " + proPath);
        }

        Reader r = new InputStreamReader(new FileInputStream(proFile), DEFAULT_ENCODING);
        props.load(r);
        return props;
    }

    public static Properties loadPropertiesUsingMap(Map content) {
        Properties props = new Properties();
        props.putAll(content);
        return props;
    }

    public String getProperty(String key) {
        String v = props.getProperty(key);
        return v == null ? "" : v;
    }
}
