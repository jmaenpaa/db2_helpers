# db2_helpers
Python helper functions to simplify management of Db2 database connections.

This package contains commands and helper functions for managing settings for Db2 databases.
Settings are saved in local files with encrypted password storage.

### Commands Included:

#### db_credentials

   Connection to Db2 is verified from prompted settings and saved.

   To protect passwords, a secret key is generated and stored in a file
   in the user's home directory. The secret key in this file can itself
   be encrypted/decrypted using the lock/unlock actions with a password.

    Actions:
      verify   - verify connection and save
      lock     - lock secret key using password
      unlock   - unlock secret key using password
      reset    - reset credentials

   Settings are stored per hostname/database in pickle files. The password
   used in the connection is stored in an encrypted format using the
   generated secret key.

   The majority of the work is done in the db2_helper.py functions.

#### db_export

   Exports data from Db2 table into CSV files.
   Does not support large objects, JSON, or XML.

#### db_import

   Import data from CSV files into Db2 tables.
   Does not support large objects, JSON, or XML.

### Functions Included:

   Functions manage saved settings for Db2 connections.

     db_connect()        - Connect using loaded/prompted credentials
     db_connect_prompt() - Prompts for connection settings
     db_connected()      - Return connection status
     db_connection()     - Return handle for current connection
     db_disconnect()     - Disconnect
     db_error()          - Handle Db2 Errors
     db_keys_get()       - Load secret key
     db_keys_lock()      - Lock secret key using password
     db_keys_set()       - Set/save secret key
     db_keys_unlock()    - Unlock secret key using password
     db_load_settings()  - Load saved settings
     db_save_settings()  - Save current settings
     db_show_settings()  - Display loaded settings
     password_to_key()   - Convert text pass-phrase to usable key for lock/unlock
     table_list()        - Get list of Db2 tables

   For security, a secret key is generated and stored in a file in the
   user's home directory. The secret key itself is not encrypted
   until the `db_keys_lock()` function is called. Once encrypted,
   the secret key can be decrypted temporarily by supplying the password
   either via the `--password` option (where applicable) or when
   prompted. The secret key can be unlocked (and saved in that state)
   using the `db_keys_unlock()` function.
