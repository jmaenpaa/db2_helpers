#!/usr/bin/env python3
"""Commands used to manage connections

   db_credentials

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

"""

import sys
import click
from db2_helpers import db_connect, db_connect_prompt, db_connected, db_disconnect, \
    db_load_settings, db_save_settings, db_show_settings, db_keys_lock, db_keys_unlock

# --------------------------------------------------
# Defaults for this program
# --------------------------------------------------
_default_schema = "db2inst1"
_default_file_location = "./db"
_default_folder_mask = 0o775


# --------------------------------------------------
# Main Function
# --------------------------------------------------
@click.command()
@click.option("--action", type=click.Choice(["verify", "lock", "unlock", "reset"], case_sensitive=False),
              help="Credentials action", default="verify", show_default=True)
@click.option("--database", "-D", help="Database Name", default="sample",
              envvar="DB_DATABASE", show_default=True)
@click.option("--hostname", "--host", "-H", help="Database Host Name", default="localhost",
              envvar="DB_HOSTNAME", show_default=True)
@click.option("--environment", "-E", help="Environment (dev/test/prod)", default="dev",
              envvar="DB_ENVIRONMENT", show_default=True)
@click.option("--password", "--pwd", "-P", help="Pass phrase for secret key (not database)", default=None)
@click.option("--show/--no-show", "-S", help="Show credentials", is_flag=True, default=False, show_default=True)
def db_credentials(action, database, hostname, environment, password, show):
    """Connect to Db2 and save credentials

   Connection to Db2 is verified from prompted settings and saved.

   To protect passwords, a secret key is generated and stored in a file
   in the user's home directory. The secret key in this file can itself
   be encrypted/decrypted using the lock/unlock actions with a password.
    """

    use_database = database.lower()
    use_hostname = hostname.lower()
    use_environment = environment.lower()

    if action == "lock":
        db_keys_lock(password)
    elif action == "unlock":
        db_keys_unlock(password)
    elif action == "reset":
        print("Reset requested, enter new credentials")
        settings = db_connect_prompt(use_database, use_hostname)
        settings["environment"] = use_environment
        db_connect(settings)
        if db_connected():
            print("Connection successful with new credentials")
            if db_save_settings(settings, password):
                print("Credentials have been saved")
            else:
                print("Credentials have not been saved")
        if show:
            db_show_settings(settings)
    elif action == "verify":
        settings = db_load_settings(use_database, use_hostname, use_environment, password)
        if settings:
            db_connect(settings)
            if db_connected():
                print("Connection credentials are correct")
                doit = False
            else:
                print("Current credentials are incorrect, enter new credentials")
                doit = True
        else:
            print("No saved credentials for", use_database.upper(),
                  "on", use_hostname.upper(),
                  "for environment", use_environment.upper())
            print("Enter credentials")
            settings = db_connect_prompt(use_database, use_hostname)
            if settings:
                doit = True
            else:
                print("Connection attempt cancelled at user request")
                sys.exit(1)

        if doit:
            db_connect(settings)
            if db_connected():
                print("Connection successful with new credentials")
                if db_save_settings(settings, password):
                    print("Credentials have been saved")
                else:
                    print("Credentials have not been saved")

        if show:
            db_show_settings(settings)

        # --------------------------------------------------
        # Clean up
        # --------------------------------------------------
        db_disconnect()

    else:
        print("Unexpected action")
        sys.exit(1)
