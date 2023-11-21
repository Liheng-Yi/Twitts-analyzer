const { MongoClient } = require('mongodb');

async function main() {
    const uri = "mongodb+srv://eli1838459978:198578ddd@cluster0.zcrzloi.mongodb.net/";
    const client = new MongoClient(uri);

    try {
        await client.connect();
        const database = client.db("ieeevisTweets");

        // Create or get the users collection
        const usersCollection = database.collection("users");
        const tweetsCollection = database.collection("tweet");

        // Extract unique users from the tweet collection
        const userPipeline = [
            {
                $group: {
                    _id: "$user.id_str", // Assuming user.id_str is the unique identifier
                    userDoc: { $first: "$user" }
                }
            },
            {
                $replaceRoot: { newRoot: "$userDoc" }
            }
        ];
        const uniqueUsers = await tweetsCollection.aggregate(userPipeline).toArray();
        await usersCollection.insertMany(uniqueUsers);
         // Drop the tweetsOnly collection if it exists
        const collections = await database.listCollections().toArray();
        const collectionNames = collections.map(c => c.name);
        if (collectionNames.includes("tweetsOnly")) {
            await database.collection("tweetsOnly").drop();
            console.log("Dropped the existing 'tweetsOnly' collection.");
        }
        // Create or get the new tweetsOnly collection
        const tweetsOnlyCollection = database.collection("tweetsOnly");

        // Iterate through the tweets and insert them into the new collection
        const tweetCursor = tweetsCollection.find();
        while (await tweetCursor.hasNext()) {
            const tweet = await tweetCursor.next();
            const modifiedTweet = {
                ...tweet,
                user_id: tweet.user.id_str // Reference to the user's ID
            };
            delete modifiedTweet.user; // Remove the embedded user document
            await tweetsOnlyCollection.insertOne(modifiedTweet);
        }

        console.log("Migration of user data and tweets completed.");

    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main().catch(console.error);
