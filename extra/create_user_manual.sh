#!/bin/sh

USAGE="Usage: DB_NAME=scife_db PORT=4000 "$0

# Check port
if [ -z "$PORT" ]; then
        echo "Please set the MongoDB PORT number."
	echo "$USAGE"
        exit -1
fi
# Check database name
if [ -z "$DB_NAME" ]; then
        echo "Please set the MongoDB DB_NAME database name."
	echo "$USAGE"
        exit -1
fi

# Get list of users
echo "Current users:"
mongo --quiet --port $PORT --eval \
	"cursor = .users.find();
	 while (cursor.hasNext() ){
		printjson(cursor.next());
	}"

# Get username and password
read -p 'Username: ' USERNAME
read -sp 'Password: ' PASSWORD
echo

# Is admin?
read -r -p "Give ADMIN privileges? [y/N] " response
case "$response" in
    [yY][eE][sS]|[yY])
        ADMIN="true"
        ;;
    *)
        ADMIN="false"
        ;;
esac

echo "Creating user '$USERNAME', admin: $ADMIN"

# Generate hashed passwor
HPASSWORD=$(echo -n "$PASSWORD" | sha512sum | cut -d " " -f 1)

# Inser user into database
JS_CREATE="""
      // Generate uuid
      function generateUUID(){
         var d = new Date().getTime();
         var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            var r = (d + Math.random()*16)%16 | 0;
            d = Math.floor(d/16);
            return (c=='x' ? r : (r&0x3|0x8)).toString(16);
         });
         return uuid;
      };

      // Check if user already exists
      var user = db.getSiblingDB('${DB_NAME}').users.findOne({username: '$USERNAME'});
      if(user) {
         print('The user already exists!');
	 quit(-1);
      }

      // Create user metadata
      var admin = $ADMIN;
      var id = generateUUID();
      var user = {
         '_id': id,
         'id': id,
         'username': '$USERNAME',
         'password': '$HPASSWORD',
         'admin': (admin ? true : false),
         'permissions': {
            'applications': [],
            'images': []
         }
      }

      // Insert into DB
      var inserted = db.getSiblingDB('${DB_NAME}').users.insert(user);
      if(!inserted) print('Failed to create user.');
      else print('User created!');
      quit(0);
"""

mongo --quiet --port $PORT --eval "$JS_CREATE"
