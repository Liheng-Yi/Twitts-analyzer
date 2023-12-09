const { MongoClient } = require('mongodb');
const { createClient } = require('redis');


async function getTweetsMongo(clientMongo, screenName) {
    const db = clientMongo.db('ieeevisTweets');
    const collection = db.collection('tweet');
    return await collection.find({ "user.screen_name": screenName }).toArray();
}

async function storeRedis(redisClient, screenName, tweets) {

    const userTweetsListKey = `tweets:${screenName}`;
    await redisClient.del(userTweetsListKey);

    for (const tweet of tweets) {
        const tweetId = tweet.id_str; 
        await redisClient.rPush(userTweetsListKey, tweetId);
        const tweetHashKey = `tweet:${tweetId}`;
        const tweetInfo = {
            user_name: tweet.user.name,
            text: tweet.text,
            created_at: tweet.created_at,
        };
        const tweetInfoPairs = Object.entries(tweetInfo).flat();
        await redisClient.hSet(tweetHashKey, tweetInfoPairs);
    }
}

async function main() {
    const redisClient = createClient({
        url: 'redis://localhost:6379' // local host in docker
    });

    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const clientMongo = new MongoClient(uri);
    // Replace with the screen name that we want 
    const screenName = 'duto_guerra'; 
    await clientMongo.connect();
    const tweets = await getTweetsMongo(clientMongo, screenName);
    await clientMongo.close();

    await redisClient.connect();
    await storeRedis(redisClient, screenName, tweets);
    await redisClient.disconnect();


}

main().catch(console.error);
