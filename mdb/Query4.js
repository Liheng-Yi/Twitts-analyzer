const { MongoClient } = require('mongodb');

async function main() {
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const client = new MongoClient(uri);

    try {
        await client.connect();

        const database = client.db("ieeevisTweets");
        const tweets = database.collection("tweet"); 
        const pipeline = [
            {
                $group: {
                    _id: "$user.screen_name",
                    avgRetweets: { $avg: "$retweet_count" },
                    tweetCount: { $sum: 1 }
                }
            },
            {
                $match: {
                    tweetCount: { $gt: 3 }
                }
            },
            {
                $sort: { avgRetweets: -1 }
            },
            {
                $limit: 10
            }
        ];

        const topUsers = await tweets.aggregate(pipeline).toArray();
        console.log("Top 10 people with the highest average retweets, who tweeted more than 3 times:");
        topUsers.forEach(user => console.log(`${user._id}: ${user.avgRetweets} average retweets`));

    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main().catch(console.error);
