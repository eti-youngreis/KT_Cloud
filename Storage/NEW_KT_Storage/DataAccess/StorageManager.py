import os
import shutil


class StorageManager:
    def __init__(self, base_directory: str):
        """
        Initialize StorageManager with a base directory for operations.
        :param base_directory: Base directory to manage files and directories within.
        """
        self.base_directory = base_directory
        if not os.path.exists(self.base_directory):
            os.makedirs(self.base_directory)

    # ---- FILE OPERATIONS ---- #

    def create_file(self, file_path: str, content: str = '') -> None:
        """
        Create a new file with optional content.
        :param file_path: Path of the file to create.
        :param content: Initial content to write to the file (default is empty).
        """
        full_path = os.path.join(self.base_directory, file_path)
        with open(full_path, 'w') as file:
            file.write(content)


    def get_content_file(self, file_path: str, part_size: int = None, offset: int = 0):
        with open(file_path, 'r') as part_file:
            part_file.seek(offset)
            if part_size:
                return part_file.read(part_size)
            return part_file.read() 


    def rename_file(self, old_file_path: str, new_file_path: str) -> None:
        """
        Rename or move a file.
        :param old_file_path: Current file path.
        :param new_file_path: New file path.
        """
        old_full_path = os.path.join(self.base_directory, old_file_path)
        new_full_path = os.path.join(self.base_directory, new_file_path)
        os.rename(old_full_path, new_full_path)


    def copy_file(self, source_file_path: str, destination_file_path: str) -> None:
        """
        Copy a file from source to destination.
        :param source_file_path: Path to the source file.
        :param destination_file_path: Path to the destination file.
        """
        source_full_path = os.path.join(self.base_directory, source_file_path)
        destination_full_path = os.path.join(self.base_directory, destination_file_path)
        shutil.copyfile(source_full_path, destination_full_path)

    
    def move_file(self, source_file_path: str, destination_file_path: str) -> None:
        """
        Move a file from source to destination.
        :param source_file_path: Path to the source file.
        :param destination_file_path: Path to the destination file.
        """
        source_full_path = os.path.join(self.base_directory, source_file_path)
        destination_full_path = os.path.join(self.base_directory, destination_file_path)
        shutil.move(source_full_path, destination_full_path)


    def delete_file(self, file_path: str) -> None:
        """
        Delete a file.
        :param file_path: Path to the file to be deleted.
        """
        full_path = os.path.join(self.base_directory, file_path)
        if os.path.exists(full_path):
            os.remove(full_path)


    def write_to_file(self, file_path: str, content: str = '', mode: str = 'w'):
        """
        Writes content to a file. If mode is 'w', it will overwrite the file.
        If mode is 'a', it will append to the file.
        """
        if mode not in ['w', 'a']:
            raise ValueError("Invalid mode. Use 'w' for overwrite and 'a' for append.")
        
        full_path = os.path.join(self.base_directory, file_path)
        if os.path.exists(full_path):
            try:
                with open(full_path, mode) as file:
                    print(file_path,"fff")
                    print(content,"jjjj")
                    file.write(content)
            except IOError as e:
                raise Exception(f"Error writing to file: {e}")
        else:
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")
        

    # ---- DIRECTORY OPERATIONS ---- #

    def create_directory(self, dir_path: str) -> None:
        """
        Create a new directory.
        :param dir_path: Path of the directory to create.
        """
        full_path = os.path.join(self.base_directory, dir_path)
        os.makedirs(full_path, exist_ok=True)


    def rename_directory(self, old_dir_path: str, new_dir_path: str) -> None:
        """
        Rename or move a directory.
        :param old_dir_path: Current directory path.
        :param new_dir_path: New directory path.
        """
        old_full_path = os.path.join(self.base_directory, old_dir_path)
        new_full_path = os.path.join(self.base_directory, new_dir_path)
        os.rename(old_full_path, new_full_path)


    def copy_directory(self, source_dir_path: str, destination_dir_path: str) -> None:
        """
        Copy a directory from source to destination.
        :param source_dir_path: Path to the source directory.
        :param destination_dir_path: Path to the destination directory.
        """
        source_full_path = os.path.join(self.base_directory, source_dir_path)
        destination_full_path = os.path.join(self.base_directory, destination_dir_path)
        shutil.copytree(source_full_path, destination_full_path)


    def move_directory(self, source_dir_path: str, destination_dir_path: str) -> None:
        """
        Move a directory from source to destination.
        :param source_dir_path: Path to the source directory.
        :param destination_dir_path: Path to the destination directory.
        """
        source_full_path = os.path.join(self.base_directory, source_dir_path)
        destination_full_path = os.path.join(self.base_directory, destination_dir_path)
        shutil.move(source_full_path, destination_full_path)


    def delete_directory(self, dir_path: str) -> None:
        """
        Delete a directory and its contents.
        :param dir_path: Path of the directory to be deleted.
        """
        full_path = os.path.join(self.base_directory, dir_path)
        if os.path.exists(full_path):
            shutil.rmtree(full_path)


    def list_files_in_directory(self, dir_path: str) -> list:
        """
        List all files in a directory.
        :param dir_path: Path of the directory to list files in.
        :return: List of file names in the directory.
        """
        full_path = os.path.join(self.base_directory, dir_path)
        return [f for f in os.listdir(full_path) if os.path.isfile(os.path.join(full_path, f))]


    def list_directories_in_directory(self, dir_path: str) -> list:
        """
        List all subdirectories in a directory.
        :param dir_path: Path of the directory to list subdirectories in.
        :return: List of subdirectory names in the directory.
        """
        full_path = os.path.join(self.base_directory, dir_path)
        return [d for d in os.listdir(full_path) if os.path.isdir(os.path.join(full_path, d))]


    def is_file_exist(self, file_path: str) -> bool:
        """
        Check if a file exists at the specified path.
        :param file_path: Path of the file to check.
        :return: True if the file exists, False otherwise.
        """
        full_path = os.path.join(self.base_directory, file_path)
        return os.path.isfile(full_path)


    def is_directory_exist(self, directory_path: str) -> bool:
        """
        Check if a directory exists at the specified path.
        :param directory_path: Path of the directory to check.
        :return: True if the directory exists, False otherwise.
        """
        full_path = os.path.join(self.base_directory, directory_path)
        return os.path.isdir(full_path)
