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
                $sort: {"user.followers_count": -1}
            },
            {
                $group: {
                    _id: "$user.screen_name",
                    followersCount: { $first: "$user.followers_count" }
                }
            },
            {
                $sort: {followersCount: -1}
            },
            {
                $limit: 10
            }
        ];
        const topUsers = await tweets.aggregate(pipeline).toArray();
        console.log("Top 10 screen_names by number of followers:");
        topUsers.forEach(user => console.log(`${user._id}: ${user.followersCount} followers`));



    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}
main().catch(console.error);
