//@ts-check
const moment = require('moment');
const {sortBy, find, filter} = require('lodash');
const axios = require('axios');
const fs = require('fs');
const mongoose = require('mongoose')
async function logData(file, data){
    return new Promise((resolve, reject) => {
        fs.appendFileSync(file, `${JSON.stringify(data)}\n`);
        resolve();
    })
}

let counter = 0;

const APILayerConfig = {
    host: 'api.faclon.com',
    port: 4000
}

// Original sensors
const lsf = 'D5';
const silica = 'D6';
const Al2O3 = 'D7';   // Aluminum Oxide sensor
const Fe2O3 = 'D10';   // Iron Oxide sensor  
const CaO = 'D11';      // Calcium Oxide sensor
const wtSensor = 'D30'; // Weight sensor remains the same

// Updated sensor list to include all five sensors
const sensorList = [lsf, silica, CaO, Fe2O3, Al2O3];
// const sensorList = [CaO, Fe2O3, Al2O3];
const prefixTagMapping = {};
sensorList.forEach(sensor => prefixTagMapping[sensor] = `${sensor}0`);

const getDataOfSensorsInTimeInterval = async (device, sensors, sTime, eTime, cursor, limit) => {
    try {
        const { host, port } = APILayerConfig;
        const path = `/getDataOfSensorsInTimeInterval`;
        const url = `http://${host}:${port}${path}`;
        const requestBody = {
            devID: device,
            sensorIDs: sensors,
            sTime,
            eTime
        };

        console.log('Requesting data from API Layer:', url, JSON.stringify(requestBody));

        const response = await axios.put(url, requestBody);
        return response.data;

    } catch (error) {
        return Promise.reject(error);
    }
}

/**
 * Computes statistical metrics (Count, Average, Standard Deviation, Weighted Count, Weighted Average)
 * for a given dataset and publishes the results via MQTT.
 *
 * @param {string} dataType - The type of data being processed, used to determine the prefix tag. Datatype can be - lsf OR silica
 * @param {Object} mqttClient - The MQTT client instance used to publish data.
 * @param {Object[]} weights - An array of weights corresponding to the data points.
 * @param {Array<{value: number, time: string}>} data - An array of data points, each containing a value and a timestamp.
 *
 * @throws {Error} If the `value` in the data array is not a valid number.
 *
 * @example
 * const dataType = "lsf";
 * const mqttClient = new MQTT.Client();
 * const weights = [0.5, 0.8, 1.2];
 * const data = [
 *   { value: 25, time: "2023-03-01T10:00:00Z" },
 *   { value: 30, time: "2023-03-01T10:01:00Z" },
 *   { value: 28, time: "2023-03-01T10:02:00Z" }
 * ];
 * computeAndPublish(dataType, mqttClient, weights, data);
 */
function computeAndPublish(sensor, data, weights) {

    try {
        // console.log('sensor chosen', sensor, 'weights length');
        const isWeightOnlySensors = [CaO, Fe2O3, Al2O3].includes(sensor);

        const prefixTag = prefixTagMapping[sensor];

        // Initialize statistical variables
        let prevSD = 0, prevAvg = 0, prevCount = 0, SD = 0, count = 0, Avg = 0;
        let weightedAvg = 0, prevWeightedAvg = 0, weightedCount = 0, prevWeightedCount = 0;

        // Prepare MQTT publish packet structure
        const publishPacket = {
            device: "FLSOLA",
            data: [],
            processed: true
        }
        
        // Process each data point sequentially to maintain running calculations
        for(let index = 0; index < data.length; index++) {

            let { value, time } = data[index];

            // for D5 and D6
            if(!isWeightOnlySensors) {

                value = Number(value);

                // Skip invalid numeric values
                if(isNaN(value)) continue;

                /**
                 * Initialize calculations for first-time sensor processing
                 * If no previous payload exists, this is the initial calculation
                 */
                if(index === 0){
                    SD = 0; Avg = value; count = 1;
                }

                else {
                    // Update running statistics using incremental formulas
                    count = prevCount + 1;
                    Avg = ( ( Number(prevAvg) * (count - 1) ) + value) / count;
                    
                    // Calculate standard deviation using incremental variance formula
                    const variance = Math.pow(prevSD, 2); // Previous variance
                    const newVariance = (1 / (count)) * (prevCount * variance + prevCount * Math.pow(prevAvg, 2) + Math.pow(value, 2)) - Math.pow(Avg, 2);
                    SD = Math.sqrt(newVariance);
                }

                // Store current values for next iteration
                prevSD = SD;
                prevAvg = Avg;
                prevCount = count;
            }

            /*********************************************/
            // Weight-based calculations section
            /*********************************************/

            const weightSensors = [];
            
            if(index < weights.length && !isNaN(Number(weights[index].value))){
                const weight = Number(weights[index].value);

                weightedCount = prevWeightedCount + weight;
                weightedAvg = ( ( prevWeightedAvg * prevWeightedCount ) + value * weight) / weightedCount;

                // Update for next iteration
                prevWeightedAvg = weightedAvg;
                prevWeightedCount = weightedCount;

                // Prepare weighted sensor data for MQTT publishing
                weightSensors.push(
                    { tag: prefixTag + '3', value: weightedCount.toFixed(2) }, 
                    { tag: prefixTag + '4', value: weightedAvg.toFixed(2) }
                );
            }

            if(!isWeightOnlySensors)
                // Tag mapping: +0=Average, +1=Count, +2=StdDev, +3=WeightedCount, +4=WeightedAvg
                publishPacket.data.push( 
                    { tag: prefixTag + '0', value: Avg.toFixed(2) },
                    { tag: prefixTag + '1', value: count.toFixed(2) },
                    { tag: prefixTag + '2', value: SD.toFixed(2) },
                    ...weightSensors
                );
            
            else publishPacket.data = weightSensors;

            // Set timestamp for MQTT packet
            publishPacket.time = moment(time).valueOf();

            require('fs').appendFileSync(`${sensor}_output.log`, JSON.stringify(publishPacket) + '\n');

            // Publish processed data to MQTT broker
            if(publishPacket.data.length > 0) {
                console.log('Publishing data', JSON.stringify(publishPacket))
                mqttClient.publish('devicesIn/FLSOLA/data', JSON.stringify(publishPacket)); // TODO: uncomment this later
            }

            // Prepare for next iteration
            // publishPacket.ISOTime = time;
            publishPacket.data = [];
        }

         // Return final statistics based on sensor type
        if(!isWeightOnlySensors) {
            return {
                prevCount: count,
                prevAvg: Avg,
                prevSD: SD,
                prevWeightedAvg: weightedAvg,
                prevWeightedCount: weightedCount
            };
        } else {
            return {
                prevWeightedAvg: weightedAvg,
                prevWeightedCount: weightedCount
            };
        }

    } catch (error) {
        console.error(`Error in computeAndPublish for sensor ${sensor}:`, error);
        return Promise.reject(error);
    }
}


const mqtt = require('mqtt');
sensorList.forEach(sensor => {
            const logFile = `${sensor}_output.log`;
            if (fs.existsSync(logFile)) {
                fs.unlinkSync(logFile);
                console.log(`Deleted log file: ${logFile}`);
            }
        });

const MQTT_HOST = 'hap.faclon.com';
// const MQTT_HOST = 'localhost';
const MQTT_PORT = 1883;
const MQTT_CLIENTID = 'influx_data_writer_' + Date.now();
const MQTT_USERNAME = 'backendServers';
const MQTT_PASSWORD = 'server$234#';
const MQTT_QOS = 1;

const options = {
    host: MQTT_HOST,
    port: MQTT_PORT,
    clientId: MQTT_CLIENTID,
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    qos: 2
};

const mqttClient = mqtt.connect(options);

mqttClient.on('connect', async () => {
    
    console.log('Connected to MQTT broker');
    try {

        await mongoose.connect('mongodb://FSVUser:Te5RDkoHOe@mongodb1.iosense.io:27017,mongodb2.iosense.io:27017,mongodb3.iosense.io:27017/productionDB');
        console.log('Connected to MongoDB: productionDB');

        const JSWStockpileLog = require('./schema');

        // const logs = await JSWStockpileLog.find({}).select('startTime endTime pileID').sort({ startTime: 1 }).lean();
        const logs = await JSWStockpileLog.find({ pileID: 'Pile_S1_80' }).select('startTime endTime pileID').sort({ startTime: 1 }).lean();
        
        for(let log of logs){

            console.log(log);
            const { startTime, endTime, pileID, status } = log;
            
            console.log(moment(startTime).valueOf(), moment(endTime).valueOf());
            console.log(moment(startTime).format('YYYY-MM-DD HH:mm:ss'), moment(endTime).format('YYYY-MM-DD HH:mm:ss'));
            
            const start =   moment(startTime).valueOf();
            const end =  endTime ? moment(endTime).valueOf() : moment().valueOf(); // Use current time if endTime is not provided
            
            console.log(start, end, pileID);
            
            let { data } = await getDataOfSensorsInTimeInterval('FLSOLA', [...sensorList, 'D30'], start, end);
            data = sortBy(data, 'time');

            if(["Disconnected"].includes(pileID)) continue;

            if(data.length === 0) {
                await logData('emptyFLSOLA.log', JSON.stringify(log));
                continue;
            }

            const logStatistics = {}; // extra added

            const weights = filter(data, { sensor: wtSensor });
            console.log(`D30 data length ===>`, weights.length);
            for(const sensor of sensorList) {
                const sensorData = filter(data, { sensor });
                console.log(`${sensor} data length ===>`, sensorData.length);
                if(sensorData.length > 0) {
                    const stats = computeAndPublish(sensor, sensorData, weights);
                    logStatistics[sensor] = stats;
                }
            }



            //**********************************************************/

            // Output the final statistics structure for this log
            console.log('\n=== Final Statistics for Log ===');
            console.log('PileID:', pileID);
            console.log(JSON.stringify(logStatistics, null, 2));
            console.log('================================\n');

            // Also save to file
            await logData('log_statistics.json', {
                pileID,
                startTime: moment(startTime).format('YYYY-MM-DD HH:mm:ss'),
                endTime: endTime ? moment(endTime).format('YYYY-MM-DD HH:mm:ss') : 'ongoing',
                statistics: logStatistics
            });

            console.log('About to wait for 10s')
            await new Promise((resolve) => setTimeout(resolve, 10000));
            console.log('Waiting season khatam')

        }

    } catch (err) {
        console.error('Error in calculateAndPublishFLSOLA:', err);
    }
});

mqttClient.on('error', (err) => {
    console.error('MQTT connection error:', err);
});

mqttClient.on('close', () => console.log('MQTT connection closed'));
mqttClient.on('offline', () => console.log('MQTT client offline'));
mqttClient.on('reconnect', () => console.log('MQTT client reconnecting'));

// module.exports = mqttClient;