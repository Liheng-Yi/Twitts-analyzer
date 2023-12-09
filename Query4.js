const { MongoClient } = require('mongodb');
const { createClient } = require('redis');

async function getTopUsersFromMongo(clientMongo) {
    const db = clientMongo.db('ieeevisTweets');
    const collection = db.collection('tweet');
    const pipeline = [
        {
            $group: {
                _id: '$user.screen_name',
                tweetCount: { $sum: 1 }
            }
        },
        {
            $sort: { tweetCount: -1 }
        },
        {
            $limit: 10
        },
        {
            $project: {
                _id: 0,
                userName: '$_id',
                tweetCount: 1
            }
        }
    ];
    return await collection.aggregate(pipeline).toArray();
}


async function updateLeaderboardInRedis(redisClient, topUsers) {
    await redisClient.del('leaderboard');
    for (const user of topUsers) {
        await redisClient.zAdd('leaderboard', {
            score: user.tweetCount,
            value: user.userName
        });
    }
    return await redisClient.ZRANGE_WITHSCORES('leaderboard', 0, 9, {
        REV: true,
    });
}
async function createLeaderboard() {
    const redisClient = createClient({
        url: 'redis://localhost:6379' 
    });
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const clientMongo = new MongoClient(uri);
    await clientMongo.connect();
    const topUsers = await getTopUsersFromMongo(clientMongo);
    await clientMongo.close();

    await redisClient.connect();
    const leaderboard = await updateLeaderboardInRedis(redisClient, topUsers);
    await redisClient.disconnect();

    console.log('Top 10 users by tweet count:', leaderboard);
}

createLeaderboard().catch(console.error);
