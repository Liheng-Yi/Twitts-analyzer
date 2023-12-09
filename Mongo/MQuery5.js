const { MongoClient } = require('mongodb');

async function main() {
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const client = new MongoClient(uri);

    try {
        await client.connect();
        const database = client.db("ieeevisTweets");
        const usersCollection = database.collection("users");
        const tweetsCollection = database.collection("tweet");
        // get unique users
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
        const uniqueUsers = await tweetsCollection.aggregate(userPipeline).toArray();
        await usersCollection.insertMany(uniqueUsers);
        const collections = await database.listCollections().toArray();
        const collectionNames = collections.map(c => c.name);
        // if the tweetsOnly collection exists, drop it first
        if (collectionNames.includes("tweetsOnly")) {
            await database.collection("tweetsOnly").drop();
            console.log("Dropped the existing 'tweetsOnly' collection.");
        }
        // create the tweetsOnly collection
        const tweetsOnlyCollection = database.collection("tweetsOnly");
        
        const tweetCursor = tweetsCollection.find();
        while (await tweetCursor.hasNext()) {
            const tweet = await tweetCursor.next();
            const modifiedTweet = {
                ...tweet,
                user_id: tweet.user.id_str 
            };
            delete modifiedTweet.user; 
            await tweetsOnlyCollection.insertOne(modifiedTweet);
        }

        console.log("tweetsOnly collection completed");

    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main().catch(console.error);
