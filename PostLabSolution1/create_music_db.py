import pymongo
from pymongo.errors import ConnectionFailure

def create_music_database():
    """
    Connects to MongoDB and creates the 'musicdb' database
    with artists and albums (with embedded tracks) using the user's data.
    """
    
    # --- Data provided by the user ---
    artists_data = [
        { "ArtistId": 4, "Name": "Radiohead" },
        { "ArtistId": 5, "Name": "Bugoy Drilon" },
        { "ArtistId": 6, "Name": "Janno Gibbs" }
    ]

    albums_data = [
        { "AlbumId": 4, "Title": "OK Computer", "ArtistId": 4 },
        { "AlbumId": 5, "Title": "Paano Na Kaya", "ArtistId": 5 },
        { "AlbumId": 6, "Title": "Silver Series", "ArtistId": 6 }
    ]

    tracks_data = [
        { "TrackId": 1, "Name": "Karma Police", "AlbumId": 4, "Composer": "Radiohead", "Milliseconds": 262000, "Bytes": 4900000, "UnitPrice": 1.29 },
        { "TrackId": 2, "Name": "Muli", "AlbumId": 5, "Composer": "Bugoy Drilon", "Milliseconds": 243000, "Bytes": 4700000, "UnitPrice": 0.99 },
        { "TrackId": 3, "Name": "Ipagpatawad Mo", "AlbumId": 6, "Composer": "Janno Gibbs", "Milliseconds": 230000, "Bytes": 4600000, "UnitPrice": 0.99 }
    ]
    
    try:
        # Connect to your local MongoDB instance
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        client.admin.command('ping')
        print("Connected to MongoDB successfully!")

        # Create or select the database
        db = client["musicdb"]

        # Drop existing collections to start fresh
        db.artists.drop()
        db.albums.drop()
        print("Dropped old 'artists' and 'albums' collections.")

        # --- Create Artists Collection ---
        artists_collection = db["artists"]
        
        # This map will store { old_ArtistId: new_mongo_ObjectId }
        artist_id_map = {} 
        
        for artist in artists_data:
            # Insert the artist doc
            insert_result = artists_collection.insert_one({
                "Name": artist["Name"]
            })
            # Map the original ArtistId (e.g., 4) to the new MongoDB _id
            artist_id_map[artist["ArtistId"]] = insert_result.inserted_id
            print(f"Inserted artist '{artist['Name']}' with new ID: {insert_result.inserted_id}")

        # --- Create Albums Collection (with Embedded Tracks) ---
        albums_collection = db["albums"]
        albums_to_insert = []

        for album in albums_data:
            # Find all tracks for the current album
            album_tracks = []
            for track in tracks_data:
                if track["AlbumId"] == album["AlbumId"]:
                    # This track belongs to the current album.
                    # We create a new doc for it, excluding the old IDs.
                    track_document = {
                        "Name": track["Name"],
                        "Composer": track["Composer"],
                        "Milliseconds": track["Milliseconds"],
                        "Bytes": track["Bytes"],
                        "UnitPrice": track("UnitPrice", 0.0) # Use .get for safety
                    }
                    album_tracks.append(track_document)
            
            # Create the full album document
            album_document = {
                "Title": album["Title"],
                # Look up the new MongoDB _id using the old ArtistId
                "artist_id": artist_id_map.get(album["ArtistId"]),
                "tracks": album_tracks
            }
            albums_to_insert.append(album_document)

        # Insert all the prepared album documents
        if albums_to_insert:
            result = albums_collection.insert_many(albums_to_insert)
            print(f"\nInserted {len(result.inserted_ids)} albums.")
        else:
            print("\nNo albums were prepared for insertion.")
            
        print("\nDatabase 'musicdb' created successfully with new data.")

    except ConnectionFailure:
        print("Could not connect to MongoDB. Please ensure it is running.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_music_database()