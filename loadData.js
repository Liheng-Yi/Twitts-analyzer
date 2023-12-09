const { createClient } = require('redis');

async function deleteAllKeys() {
    const client = createClient({
        url: 'redis://localhost:6379' // Replace with your Redis server URL if different
    });

    client.on('error', (err) => console.log('Redis Client Error', err));

    try {
        await client.connect();

        if (client.isReady) {
            // Delete all keys in all databases
            await client.flushAll();
            console.log('All keys in all databases have been deleted.');

            await client.disconnect();
        } else {
            console.log('Redis client is not ready.');
        }
    } catch (error) {
        console.error('Error:', error);
    }
}

const fs = require('fs');
const readline = require('readline');


// Function to import data to Redis
async function importDataToRedis(filePath) {
    const client = createClient({
        url: 'redis://localhost:6379' // Adjust as necessary
    });

    client.on('error', (err) => console.log('Redis Client Error', err));

    try {
        await client.connect();

        const fileStream = fs.createReadStream(filePath);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });

        for await (const line of rl) {
            try {
                const record = JSON.parse(line);
                // Assuming 'id' is the unique identifier for each record
                const key = `record:${record.id}`;
                await client.set(key, JSON.stringify(record));
            } catch (parseError) {
                console.error('Error parsing JSON:', parseError);
            }
        }

        console.log('Data import complete.');
        await client.disconnect();
    } catch (error) {
        console.error('Error:', error);
    }
}

// Replace 'path/to/your/file.dump' with the path to your .dump file

deleteAllKeys();
importDataToRedis('./ieeevis2020Tweets.dump');
