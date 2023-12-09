const { MongoClient } = require('mongodb');

async function main() {
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const client = new MongoClient(uri);

    try {
        await client.connect();
        const database = client.db("ieeevisTweets");
        const tweetsCollection = database.collection("tweet");
        const usersCollection = database.collection("users");

        // Aggregate unique users from the tweets collection
        const userPipeline = [
            {
                $group: {
                    _id: "$user.id_str", 
                    userDoc: { $first: "$user" }
                }
            },
            {
                $replaceRoot: { newRoot: "$userDoc" }
            }
        ];

        

    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main().catch(console.error);
