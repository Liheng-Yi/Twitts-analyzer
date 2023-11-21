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
                    _id: "$user.id_str", // Assuming 'id_str' is a unique identifier for users
                    userDoc: { $first: "$user" }
                }
            },
            {
                $replaceRoot: { newRoot: "$userDoc" }
            }
        ];

        const uniqueUsers = await tweetsCollection.aggregate(userPipeline).toArray();

        // Insert unique users into the users collection
        if(uniqueUsers.length > 0) {
            await usersCollection.insertMany(uniqueUsers);
            console.log(`Inserted ${uniqueUsers.length} unique users into the users collection.`);
        } else {
            console.log("No unique users found in tweets collection.");
        }

    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main().catch(console.error);
