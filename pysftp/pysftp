import pysftp

# Create credentials.py to store sftp credentials as dict object
import credentials as cred


sftp = pysftp.Connection(
        'hostname',
        username = cred.dict_obj['username'],
        password = cred.dict_obj['password'],
        private_key = 'absolute_filepath_for_sshkey.ppk'
)

# Change working directory once connection is established
with sftp.cd('/parent_dir'):
        sftp.chdir('child_dir')
        sftp.chdir('grandchild_dir')

        # Put the my_csv.csv file into specified directory
        sftp.put('/absolute_filepath/my_csv.csv', preserve_mtime=True)

        # Check that it wsa successfully put there
        assert sftp.exists('my_csv.csv'), 'Upload unsuccessful'

# Close connection
sftp.close()

