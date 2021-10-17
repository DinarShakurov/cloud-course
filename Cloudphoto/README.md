# Cloudphoto
___

### Подготовка:
1. Перед запуском приложения необходимо установить следующие зависимости:
   - boto3 
    ```shell
    pip install boto3
    ```
2. В конфигурационном файле ```.\config.ini``` проставить 
```bucket_name```, ```aws_access_key_id```, ```aws_secret_access_key```
___
### Запуск:
Пример:
```shell
python cloudphoto.py upload -p "Путь/до/директории" -a "Название альбома" 
```