const { MongoClient } = require('mongodb');
const { createClient } = require('redis');

async function getUsersInMongo(uri, dbName, collectionName) {
    const clientMongo = new MongoClient(uri);
    try {
        await clientMongo.connect();
        console.log('Connected to MongoDB');

        const collection = clientMongo.db(dbName).collection(collectionName);
        return await collection.distinct("user.screen_name");
    } catch (error) {
        console.error('MongoDB Error:', error);
        throw error;
    } finally {
        await clientMongo.close();
    }
}
async function countingInRedis(usernames) {
    const redisClient = createClient({
        url: 'redis://localhost:6379' // Adjust as necessary
    });
    try {
        await redisClient.connect();
        console.log('Connected to Redis');

        for (const screenName of usernames) {
            await redisClient.sAdd('screen_names', screenName);
        }
        return await redisClient.sCard('screen_names');
    } catch (error) {
        console.error('Redis Error:', error);
        throw error;
    } finally {
        await redisClient.disconnect();
    }
}

async function main() {
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const dbName = 'ieeevisTweets'; 
    const collectionName = 'tweet';

    try {
        const distinctUsernames = await getUsersInMongo(uri, dbName, collectionName);
        const distinctUserCount = await countingInRedis(distinctUsernames);
        console.log(`Number of distinct users: ${distinctUserCount}`);
    } catch (error) {
        console.error('Error in main function:', error);
    }
}

main().catch(console.error);
