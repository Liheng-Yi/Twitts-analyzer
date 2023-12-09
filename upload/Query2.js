const { MongoClient } = require('mongodb');
const { createClient } = require('redis');

async function countTotalFavorites() {
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

        // Initialize favoritesSum in Redis
        await redisClient.set('favoritesSum', 0);
        
        // Connect to the MongoDB collection
        const db = MongoCli.db(dbName);
        const collection = db.collection(collectionName);

        // Get a cursor to iterate over all tweets in MongoDB
        const cursor = collection.find();

        // Iterate over all tweets and sum up the favorites
        while (await cursor.hasNext()) {
            const tweet = await cursor.next();
            const favoriteCount = tweet.favorite_count || 0; // Assuming the field is favorite_count
            await redisClient.incrBy('favoritesSum', favoriteCount);
        }

        // Get the final value of favoritesSum from Redis
        const finalFavoritesSum = await redisClient.get('favoritesSum');
        console.log(`Total number of favorites: ${finalFavoritesSum}`);

    } catch (error) {
        console.error('Error:', error);
    } finally {
        // Close the connections
        await redisClient.disconnect();
        await MongoCli.close();
    }
}

countTotalFavorites();
