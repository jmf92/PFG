package pfg.Util;

import org.ho.yaml.Yaml;
import org.ho.yaml.exception.YamlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Created by jmf on 19/08/15.
 */
public class ConfigData {
    private static final Logger log = LoggerFactory.getLogger(ConfigData.class);

    private Map map;
    public ConfigData(){}

    public Map load(String configFile) {

        try {
            this.map = (Map<String, Object>) Yaml.load(new File(configFile));
        } catch (FileNotFoundException e) {
            log.error("Couldn't find config file {}", configFile);
            System.exit(1);
        } catch (YamlException e) {
            log.error("Couldn't read config file {}. Is it a YAML file?", configFile);
            System.exit(1);
        }
    return map;
    }
}
