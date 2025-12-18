const axios = require("axios");
const moment = require("moment");
const fs = require("fs");
const path = require("path");

const PUT_API_URL = "http://api.faclon.com:4000/getDataOfSensorsInTimeInterval";

const GET_LAST_DP_URL = "http://api.faclon.com:4000/lastDP?device=ABBLSCRSHR_A1&sensor=D0";

// ✅ Hardcoded payload
const payload = {
  devID: "ABBLSCRSHR_A1",
  sensorIDs: ["D0", "D3"],
  sTime: 1763865947,
  // eTime: moment().unix(),
  eTime: 1764000000,
};

const pileID = 'Pile_S1_80';

async function fetchSensorData() {
  try {
    // ================================
    // ✅ 1️⃣ FETCH WINDOW DATA (PUT)
    // ================================
    const response = await axios.put(PUT_API_URL, payload, {
      headers: { "Content-Type": "application/json" },
    });

    const rawData = response.data?.data || [];

    const groupedMap = {};

    for (const item of rawData) {
      const unixMs = moment(item.time).valueOf();

      if (!groupedMap[unixMs]) {
        groupedMap[unixMs] = { time: unixMs };
      }

      groupedMap[unixMs][item.sensor] = Number(item.value);
    }

    let windowArr = Object.values(groupedMap);

    // ✅ Sort & get last 15
    windowArr.sort((a, b) => a.time - b.time);
    const windowLast15 = windowArr.slice(-15);

    // ✅ STRINGIFIED WINDOW
    const windowString = JSON.stringify(windowLast15);

    // ================================
    // ✅ 2️⃣ FETCH LAST DP (GET)
    // ================================
    const lastDPResponse = await axios.get(GET_LAST_DP_URL);
    const lastDP = lastDPResponse.data?.[0]?.[0];

    if (!lastDP) {
      console.log("❌ No lastDP found");
      return;
    }

    const lastTimeUnixMs = moment(lastDP.time).valueOf();

    // ================================
    // ✅ 3️⃣ FINAL REQUIRED OUTPUT
    // ================================
    const finalOutput = {
      time: lastTimeUnixMs,
      window: windowString,
      D0: 0,
      D3: 1,
      pileID,
      status: "Running",
      pileChange: {
        hasPileChanged: true,
        pileStartTime: 1763865947000,
      },
    };

    console.log("\n✅ FINAL OUTPUT OBJECT:\n", finalOutput);

    // ================================
    // ✅ 4️⃣ WRITE TO JSON FILE
    // ================================
    const filePath = path.join(__dirname, "output.json");

    fs.writeFileSync(
      filePath,
      JSON.stringify(finalOutput, null, 2), // ✅ Pretty format
      "utf-8"
    );

    console.log(`\n✅ JSON file written successfully at:\n${filePath}`);

  } catch (error) {
    console.error("❌ API Error:", error?.response?.data || error.message);
  }
}

// ✅ Run the function
fetchSensorData();
