import yaml


def return_content(path):
    with open(path, 'r') as stream:
        try:
            yaml_content = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
        except Exception as ex:
            print(ex)
        else:
            return yaml_content
