# db2_helpers
Python helper functions to simplify management of Db2 database connections.

This package contains commands and helper functions for managing settings for Db2 databases.
Settings are saved in local files with encrypted password storage. 
More than one concurrent database connections is **not** supported at this time.

## Installation

Install using pip or clone the repository and modify to your heart's content.

```bash
    pip install db2_helpers
```

PyPI page is [here](https://pypi.org/project/db2-helpers/) 
and original source repository is [here](https://github.com/jmaenpaa/db2_helpers)

## Commands Included:

### db_credentials

   Used to set Db2 database connection attributes and save them for later use
   in programs that need to connect to Db2. Connection settings are saved
   in local files with the connection password encrypted. The encryption
   key is stored in a hidden file in the user's home directory (default).

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

### db_export

   Exports data from Db2 table into CSV files.
   Does not support large objects, JSON, or XML.

### db_import

   Import data from CSV files into Db2 tables.
   Does not support large objects, JSON, or XML.

## Functions Included:

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

## Environments

   The functions and commands support the specification of an Environment allowing
   flexible use of development, test, production settings. These can be specified
   as command line options or environment variables (for the commands).
   Connection settings are unique for the combination of Environment, Hostname, Database.
   
### Environment Variables

   #### DB_ENVIRONMENT
   
   Environment name do use in naming connection settings files (default is `dev`).
 
   #### DB_HOSTNAME

   Host name for database connection (default is `localhost`).
   
   #### DB_DATABASE

   Database name for connection (default is `sample`).
   
### Command Options

   #### --environment, --hostname, --database

   The `--environment`, `--hostname`, `--database` command options correspond
   to the environment, hostname, and database environment needed to specify a
   unique connection.

   #### --password

   Supply a password for locking/unlocking your secret key. 
   If not supplied, you will be prompted where necessary.

   #### --action

   Controls the action that will be performed by the `db_credentials` command.

   | Action | Description |
   | verify | Verify connection and prompt for new connection settings if needed (default) |
   | reset  | Ignore saved settings and prompt for new ones |
   | lock   | Lock the secret encryption key using `password` |
   | unlock | Unlock the secret encryption key using `password` |

   #### --show

   Display credentials after verification.

   #### --help

   Display help informatin for the command.

## Recommendations

Use Python virtual environments when installing.

Install `ibm_db` in the virtual environment. 
If you are using macOS then this may be the only way to get `ibm_db` to work as there are issues when using the IBM Db2 Data Server Driver (DSDriver) client on macOS.
