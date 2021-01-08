"""Database helper functions for single database connectivity

   Manage saved settings for Db2 connections
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

   The secret key is generated and stored in a file in the
   user's home directory. The secret key itself is not encrypted
   until the db_keys_lock() function is called. Once encrypted,
   the secret key can be decrypted temporarily by supplying the password
   either via the --password option (where applicable) or when
   prompted. The secret key can be unlocked (and saved in that state)
   using the db_keys_unlock() function.

"""


import base64
import collections
import os
import pickle
import stat
from pathlib import Path
from getpass import getpass
from hashlib import blake2b
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.fernet import Fernet
import ibm_db

_hdbc = None
_sqlerror = None
_sqlcode = None
_sqlstate = None

_default_environment = "dev"
_default_settings_location = Path("")  # Location for dev_host_db.pickle files
_default_secret_key_location = Path.home()  # Location of secret key file (user's home directory)

_secretkeyfile = _default_secret_key_location / ".db2_helpers.secret.key"

_default_secretkey = collections.OrderedDict([
    ("secret", None),
    ("locked", False),
    ("hash", ""),
    ("secrethash", "")
])

_default_settings = collections.OrderedDict([
    ("database", "sample"),
    ("hostname", "localhost"),
    ("protocol", "tcpip"),
    ("port", "50000"),
    ("uid", "db2inst1"),
    ("pwd", "password"),
    ("environment", _default_environment),
    ("security", "nossl"),
    ("servercert", "db2inst1.arm"),
    ("secrethash", "")  # Hash of secret key used to encrypt password
])

_settings = _default_settings.copy()

_prompt_label = collections.OrderedDict([
    ("database", "database name"),
    ("hostname", "host name for database"),
    ("protocol", "protocol for database"),
    ("port", "port for tcpip connection"),
    ("uid", "userid for database connection"),
    ("pwd", "password for database connection"),
    ("servercert", "certificate file for database"),
])


def db_connect(settings: collections.OrderedDict = None) -> ibm_db.IBM_DBConnection or None:
    """Connect to Db2"""

    global _hdbc

    if _hdbc and db_connected():
        return _hdbc
    if not settings["database"]:
        print("Settings are incorrect")
        _hdbc = None
        return _hdbc

    if "security" in settings and settings["security"].upper() == "SSL":
        dsn = (
            "DRIVER={{IBM DB2 ODBC DRIVER}};"
            "DATABASE={0};"
            "HOSTNAME={1};"
            "PORT={2};"
            "PROTOCOL=TCPIP;"
            "UID={3};"
            "PWD={4};"
            "SECURITY=SSL;SSLServerCertificate={5}").format(settings["database"],
                                                            settings["hostname"],
                                                            settings["port"],
                                                            settings["uid"],
                                                            settings["pwd"],
                                                            settings["servercert"])
    else:
        dsn = (
            "DRIVER={{IBM DB2 ODBC DRIVER}};"
            "DATABASE={0};"
            "HOSTNAME={1};"
            "PORT={2};"
            "PROTOCOL=TCPIP;"
            "UID={3};"
            "PWD={4};").format(settings["database"],
                               settings["hostname"],
                               settings["port"],
                               settings["uid"],
                               settings["pwd"])

    # Get a database handle (hdbc) for subsequent access to DB2
    try:
        _hdbc = ibm_db.connect(dsn, "", "")
    except Exception as err:
        print(str(err))
        _hdbc = None

    return _hdbc


def db_connect_prompt(database=None, hostname=None) -> collections.OrderedDict or None:
    """Prompt for connection settings, do not actually connect"""

    global _default_settings, _prompt_label

    settings = _default_settings.copy()

    if database:
        settings["database"] = database
    if hostname:
        settings["hostname"] = hostname

    print("Enter the database connection details (Enter a period '.' to cancel input")

    for k in settings.keys():
        if k in ["servercert", "hash", "secrethash", "environment"]:
            pass
        elif k == "pwd":
            x = getpass("Enter password: ")
            if x == ".":
                return None
            if x:
                settings[k] = x
        elif k == "security":
            prompt_string = "Enter 'SSL' to use an encrypted connection[" + settings[k] + "]: "
            x = input(prompt_string).lower() or settings[k]
            if x == ".":
                return None
            m = "servercert"
            settings[k] = x
            if x == "ssl":
                y = input("Enter the name of the .ARM file containing the server certificate["
                          + settings[m] + "]: ") or settings[m]
                if y == ".":
                    return None
                z = Path(y)
                if z.is_file() and os.access(y, os.R_OK):
                    settings[m] = y
                else:
                    print("Unable to access file", z)
                    return None
            else:
                settings[m] = ""
        else:
            prompt_string = "Enter the " + _prompt_label[k] + "[" + settings[k] + "]: "
            x = input(prompt_string)
            if x == ".":
                return None
            if x:
                settings[k] = x.lower()

    return settings


def db_connected(hdbc=None) -> bool:
    """ Return state of Db2 connection"""
    global _hdbc
    if hdbc:
        return ibm_db.active(hdbc)
    if _hdbc:
        return ibm_db.active(_hdbc)
    return False


def db_connection() -> ibm_db.IBM_DBConnection or None:
    """ Return Db2 connection handle"""
    global _hdbc
    return _hdbc


# noinspection PyBroadException
def db_disconnect(hdbc=None):
    """Disconnect from the database"""
    if hdbc:
        use_hdbc = hdbc
    else:
        use_hdbc = _hdbc
    try:
        ibm_db.close(use_hdbc)
    except Exception:
        db_error(False)


# noinspection PyBroadException
def db_error(quiet):
    """Handle Db2 Errors"""

    global _sqlerror, _sqlcode, _sqlstate

    errmsg = ibm_db.stmt_errormsg().replace("\r", " ")
    errmsg = errmsg[errmsg.rfind("]") + 1:].strip()
    _sqlerror = errmsg

    msg_start = errmsg.find("SQLSTATE=")
    if msg_start != -1:
        msg_end = errmsg.find(" ", msg_start)
        if msg_end == -1:
            msg_end = len(errmsg)
        _sqlstate = errmsg[msg_start + 9:msg_end]
    else:
        _sqlstate = "0"

    msg_start = errmsg.find("SQLCODE=")
    if msg_start != -1:
        msg_end = errmsg.find(" ", msg_start)
        if msg_end == -1:
            msg_end = len(errmsg)
        _sqlcode = errmsg[msg_start + 8:msg_end]
        try:
            _sqlcode = int(_sqlcode)
        except Exception:
            pass
    else:
        _sqlcode = 0

    if quiet:
        return

    print(errmsg)


def db_keys_get(password=None, prompt=True) -> collections.OrderedDict:
    """Load saved secret key"""

    global _secretkeyfile, _default_secretkey
    passphrase = ""
    try:
        with open(_secretkeyfile, "rb") as f:
            secretkey = pickle.load(f)
        if secretkey["locked"]:
            getit = True
            if password:
                passphrase = password
                if secretkey["hash"] == blake2b(str.encode(passphrase)).hexdigest():
                    print("Secret key file is locked.")
                    print("Using supplied password for temporary unlock.")
                else:
                    print("Secret key file is locked.")
                    print("Supplied unlock password does not match secret")
            elif prompt:
                print("Secret key file is locked.")
                print("No secret password supplied")
                attempts = 0
                while getit:
                    attempts += 1
                    if attempts > 9:
                        getit = False
                    passphrase = getpass("Enter password: ")
                    if secretkey["hash"] == blake2b(str.encode(passphrase)).hexdigest():
                        getit = False
            k = Fernet(password_to_key(passphrase))
            secretkey["secret"] = k.decrypt(str.encode(secretkey["secret"])).decode()
            secretkey["locked"] = False
    except FileNotFoundError:
        print("Secret key file does not exist, creating new one")
        secretkey = _default_secretkey.copy()
        secretkey = db_keys_set(secretkey, True)
    return secretkey


# noinspection PyBroadException
def db_keys_lock(passphrase) -> bool:
    """Lock secret key with a pass phrase"""

    global _secretkeyfile
    try:
        with open(_secretkeyfile, "rb") as f:
            secretkey = pickle.load(f)
            if secretkey["locked"]:
                print("Secret key file is already locked")
                return True
            if passphrase:
                usepass = passphrase
            else:
                usepass = getpass("Enter pass phrase: ")
                usepass2 = getpass("Enter pass phrase again: ")
                print("")
                if usepass != usepass2:
                    print("Pass phrase mismatch, secret key still unlocked")
                    return False
            if usepass:
                k = Fernet(password_to_key(usepass))
                secretkey["secret"] = k.encrypt(str.encode(secretkey["secret"])).decode()
                secretkey["locked"] = True
                secretkey["hash"] = blake2b(str.encode(usepass)).hexdigest()
            db_keys_set(secretkey, False)
    except Exception:
        print("Error locking secret key content")
        return False
    print("Secret key successfully locked")
    return True


# noinspection PyBroadException
def db_keys_set(secretkey: collections.OrderedDict, newkey=False) -> collections.OrderedDict:
    """Save secret key with option to generate a new one"""

    global _secretkeyfile
    global _default_secretkey

    if newkey:
        secret = Fernet.generate_key()  # Create new secret
        secrethash = blake2b(secret).hexdigest()
        secretkey = _default_secretkey
        secretkey["secret"] = secret.decode()
        secretkey["locked"] = False
        secretkey["hash"] = None
        secretkey["secrethash"] = secrethash
    try:
        with open(_secretkeyfile, "wb") as f:
            pickle.dump(secretkey, f)
    except PermissionError:
        print("Failed trying to write secret key file (permissions).")
        return collections.OrderedDict()
    except FileNotFoundError:
        print("Failed trying to write secret key file (not found).")
        return collections.OrderedDict()
    try:
        os.chmod(_secretkeyfile, stat.S_IRUSR | stat.S_IWUSR)
    except PermissionError:
        print("Failed setting permissions on secret key file.")
        return collections.OrderedDict()
    return secretkey


# noinspection PyBroadException
def db_keys_unlock(passphrase) -> bool:
    """Unlock secret key with pass phrase"""
    global _secretkeyfile

    try:
        with open(_secretkeyfile, "rb") as f:
            secretkey = pickle.load(f)
        if not secretkey["locked"]:
            print("Secret key file is already unlocked")
            return True
        if passphrase:
            usepass = passphrase
        else:
            usepass = getpass("Enter pass phrase: ")
            print("")
        if usepass:
            if secretkey["hash"] == blake2b(str.encode(usepass)).hexdigest():
                k = Fernet(password_to_key(usepass))
                secretkey["secret"] = k.decrypt(str.encode(secretkey["secret"])).decode()
                secretkey["locked"] = False
                db_keys_set(secretkey, False)
            else:
                print("Pass phrase did not match, secret key remains locked")
                return False
    except Exception:
        print("Error locking secret key content")
        return False
    print("Secret key successfully unlocked")
    return True


# noinspection PyBroadException
def db_load_settings(database, hostname, environment=_default_environment,
                     password=None) -> collections.OrderedDict or None:
    """Load saved settings"""

    global _default_settings_location

    keys = db_keys_get(password)
    fname = _default_settings_location / str(
        environment.lower() + "_" + hostname.lower() + "_" + database.lower() + ".pickle")
    try:
        with open(fname, "rb") as f:
            settings = pickle.load(f)
        if keys:
            if settings["secrethash"] == keys["secrethash"]:
                k = Fernet(str.encode(keys["secret"]))
                settings["pwd"] = k.decrypt(str.encode(settings["pwd"])).decode()
            else:
                print("Saved settings are incorrect, wrong secret key")
                return None
    except Exception:
        return None
    return settings


def db_save_settings(settings: collections.OrderedDict, password=None) -> bool:
    """Save settings"""

    global _default_secretkey
    use_settings = settings.copy()

    keys = db_keys_get(password)
    if not keys or "secret" not in keys or not keys["secret"]:
        print("Setting up new secret key file")
        keys = db_keys_set(_default_secretkey, True)
    use_settings["secrethash"] = keys["secrethash"]
    k = Fernet(str.encode(keys["secret"]))
    use_settings["pwd"] = k.encrypt(str.encode(use_settings["pwd"])).decode()

    fname = _default_settings_location / str(
        use_settings["environment"].lower() + "_" + use_settings["hostname"].lower() + "_" + use_settings[
            "database"].lower() + ".pickle")
    try:
        with open(fname, "wb") as f:
            pickle.dump(use_settings, f)
    except PermissionError:
        print("Failed trying to write credentials file.")
        return False
    try:
        os.chmod(fname, stat.S_IRUSR | stat.S_IWUSR)
    except PermissionError:
        print("Failed setting permissions on credentials file.")
        return False
    return True


def db_show_settings(settings: collections.OrderedDict):
    """Show current connection settings"""
    if settings:
        print("Credentials for", settings["database"].upper(),
              "on", settings["hostname"].upper(),
              "for environment", settings["environment"].upper())
        for k, v in settings.items():
            if k == "pwd":
                if settings[k]:
                    print("password: [not displayed]")
                else:
                    print("password: [no password]")
            else:
                print(k + ":", v)
    else:
        print("Settings have not been loaded")


def password_to_key(passphrase):
    """Convert passphrase to Fernet compatible key"""

    password = str.encode(passphrase)
    salt = b'2390489409578390'  # Use fixed salt, don't store the result
    # noinspection PyArgumentList
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))
    return key


def table_list(schema, allow_views=False) -> []:
    """Get list of tables in schema"""
    global _hdbc
    temp_list = []

    if allow_views:
        sqlcat = """select distinct tabname
                      from syscat.tables
                     where tabschema = ?
                     order by tabname;"""
    else:
        sqlcat = """select distinct tabname
                      from syscat.tables
                     where tabschema = ?
                       and type = 'T'
                     order by tabname;"""

    try:
        stmtcat = ibm_db.prepare(_hdbc, sqlcat)
        parameters = (str(schema.upper()),)

        if ibm_db.execute(stmtcat, parameters):
            cat_row = ibm_db.fetch_assoc(stmtcat)
            while cat_row:
                # export_table(str(schema.upper()), cat_row["TABNAME"])
                temp_list.append(cat_row["TABNAME"])
                cat_row = ibm_db.fetch_assoc(stmtcat)

        ibm_db.free_stmt(stmtcat)

    except Exception as err:
        print(err)
        db_error(False)
        return None

    return temp_list
