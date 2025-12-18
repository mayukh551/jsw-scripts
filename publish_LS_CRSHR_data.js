const axios = require("axios");
const moment = require("moment");
const mqtt = require("mqtt");

// ================================
// ✅ API CONFIG
// ================================
const API_URL =
  "http://api.faclon.com:4000/getDataOfSensorsInTimeInterval";

const payload = {
  devID: "ABBLSCRSHR_A1",
  sensorIDs: ["D0", "D3"],
  sTime: moment.unix(1763999957).add(1, 'minute').unix(),
  eTime: moment().unix(),
};

// ================================
// ✅ MQTT CONFIG (CHANGE IF NEEDED)
// ================================
const MQTT_BROKER_URL = "mqtt://localhost:1883"; // ✅ Change if needed
const MQTT_TOPIC = "devicesIn/ABBLSCRSHR_A1/data";

// ================================
// ✅ MQTT CONNECTION
// ================================
const client = mqtt.connect(MQTT_BROKER_URL);

client.on("connect", async () => {
  console.log("✅ Connected to MQTT Broker");

  try {
    const response = await axios.put(API_URL, payload, {
      headers: { "Content-Type": "application/json" },
    });

    const rawData = response.data?.data || [];

    /**
     * ✅ Group by timestamp
     */
    const groupedMap = {};

    for (const item of rawData) {
      const unixMs = moment(item.time).valueOf();

      if (!groupedMap[unixMs]) {
        groupedMap[unixMs] = {
          device: payload.devID,
          time: unixMs,
          data: [],
        };
      }

      groupedMap[unixMs].data.push({
        tag: item.sensor,
        value: Number(item.value),
      });
    }

    /**
     * ✅ Convert to final output array
     */
    const finalOutputArray = Object.values(groupedMap);

    console.log("\n✅ PUBLISHING TO MQTT:\n");

    /**
     * ✅ Publish EACH timestamp as a separate message
     */
    for (const msg of finalOutputArray) {
      const mqttPayload = JSON.stringify(msg);

      client.publish(MQTT_TOPIC, mqttPayload, { qos: 1 }, (err) => {
        if (err) {
          console.error("❌ MQTT Publish Error:", err);
        } else {
          console.log("✅ Published:", mqttPayload);
        }
      });
    }

  } catch (error) {
    console.error("❌ API Error:", error?.response?.data || error.message);
  }
});

// ================================
// ✅ MQTT ERROR HANDLING
// ================================
client.on("error", (err) => {
  console.error("❌ MQTT Connection Error:", err);
});
