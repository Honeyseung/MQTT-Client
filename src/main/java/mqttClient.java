import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class mqttClient {
  public static void main(String[] args) {
    final Logger logger = LoggerFactory.getLogger(mqttClient.class);

    Config config = ConfigFactory.load();
    String configPath = System.getProperty("config.file");

    if (configPath != null) {
      File f = new File(configPath);
      if (f.exists() && !f.isDirectory()) {
        config = ConfigFactory.parseFile(f).resolve();
        logger.info("Configuration is loaded. [{}]", f);
      } else {
        logger.error("Failed to load configuration. Please check the [-Dconfig.file] option.");
        System.exit(0);
      }
    } else {
      logger.debug("Configuration is loaded. (Development Mode)");
    }

    MemoryPersistence persistence = new MemoryPersistence();

    String host = config.getString("host");
    String topic = config.getString("topic");
    int qos = 0;
    String broker = "tcp://" + host;
    String clientId = config.getString("clientId");
    Gson gson = new Gson();

    try {
      // Publisher
      MqttClient publisherClient = new MqttClient(broker, clientId, persistence);
      MqttConnectOptions connOpts = new MqttConnectOptions();
      connOpts.setCleanSession(true);
      connOpts.setUserName(config.getString("mqtt-id"));
      connOpts.setPassword(config.getString("mqtt-pw").toCharArray());
      String path = config.getString("file-path");
      logger.info("Connecting to broker: {}", broker);
      publisherClient.connect(connOpts);
      logger.info("Connected");

      List<String> logs = config.getStringList("logs");

      for (String log : logs) {
        Timer timer = new Timer();
        int interval = config.getInt(log.toLowerCase() + "-interval");
        timer.schedule(
            new TimerTask() {
              int index = 0;
              List<List<Object>> jsonList = null;
              List<Object> logList = null;

              @Override
              public void run() {
                if (jsonList == null) {
                  try (Reader reader = new FileReader(path + "/" + log.toLowerCase() + ".json")) {
                    jsonList = gson.fromJson(reader, new TypeToken<List<Object>>() {}.getType());
                    logger.info("Read {} {} objects from JSON file.", jsonList.size(), log);
                  } catch (JsonSyntaxException | JsonIOException | IOException e) {
                    logger.error(
                        "Failed to read {} objects from JSON file: {}", log, e.getMessage());
                    System.exit(0);
                  }
                }
                List<Object> logList = jsonList.get(index++);
                if (index >= jsonList.size()) {
                  index = 0;
                }

                for (Object obj : logList) {
                  String json = gson.toJson(obj);
                  JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
                  long logEntDt = System.currentTimeMillis();
                  jsonObject.addProperty("logEntDt", logEntDt);
                  if (log.equals("SysLog")) {
                    long convertedToLong = jsonObject.get("hwMfDt").getAsLong();
                    jsonObject.addProperty("hwMfDt", convertedToLong);
                    convertedToLong = jsonObject.get("osUpdDt").getAsLong();
                    jsonObject.addProperty("osUpdDt", convertedToLong);
                  }
                  String updatedJson = gson.toJson(jsonObject);
                  logger.info("Publishing message: {}-{}", log, updatedJson);
                  MqttMessage message = new MqttMessage(updatedJson.getBytes());
                  message.setQos(qos);
                  try {
                    publisherClient.publish(topic, message);
                  } catch (MqttException e) {
                    logger.error("Failed to publish message: {}", e.getMessage());
                  }
                }
              }
            },
            0,
            interval * 1000);
      }
    } catch (MqttException e) {
      logger.error("Failed to connect to broker: {}", e.getMessage());
    }
  }
}
