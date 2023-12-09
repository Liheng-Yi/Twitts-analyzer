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
                    tweetCount: { $sum: 1 }
                }
            },
            {
                $sort: {tweetCount: -1}
            },
            {
                $limit: 1
            }
        ];
        const result = await tweets.aggregate(pipeline).toArray();
        console.log(`User's name: ${result[0]._id} number of tweets ${result[0].tweetCount}.`);


    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main().catch(console.error);
