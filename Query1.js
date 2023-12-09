const { MongoClient } = require('mongodb');
const { createClient } = require('redis');

async function countTweetsAndStoreInRedis() {
    const redisClient = createClient({
        url: 'redis://localhost:6379' // Adjust as necessary
    });
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";

    const MongoCli = new MongoClient(uri);

    const dbName = 'ieeevisTweets'; // Replace with your database name
    const collectionName = 'tweet'; // Replace with your collection name

    try {
        // Connect to Redis and MongoDB
        await redisClient.connect();
        await MongoCli.connect();

        console.log('Connected to Redis and MongoDB');

        // Initialize tweetCount in Redis
        await redisClient.set('tweetCount', 0);
        
        // Connect to the MongoDB collection
        const db = MongoCli.db(dbName);
        const collection = db.collection(collectionName);

        // Count the number of tweets in MongoDB
        const tweetCountMongo = await collection.countDocuments();

        // Increase tweetCount in Redis
        for (let i = 0; i < tweetCountMongo; i++) {
            await redisClient.incr('tweetCount');
        }

        // Get the final value of tweetCount from Redis
        const finalTweetCount = await redisClient.get('tweetCount');
        console.log(`There were ${finalTweetCount} tweets`);

    } catch (error) {
        console.error('Error:', error);
    } finally {
        // Close the connections
        await redisClient.disconnect();
        await MongoCli.close();
    }
}

countTweetsAndStoreInRedis();
