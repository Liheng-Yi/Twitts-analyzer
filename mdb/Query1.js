const { MongoClient } = require('mongodb');

async function main() {
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const client = new MongoClient(uri);

    try {
        await client.connect();
        
        const database = client.db("ieeevisTweets"); 
        const tweets = database.collection("tweet"); 
        const query = {
            retweeted_status: { $exists: false }
        };
        const count = await tweets.countDocuments(query);
        console.log(`Number of tweets that are not retweets or replies: ${count}`);
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}
main().catch(console.error);
