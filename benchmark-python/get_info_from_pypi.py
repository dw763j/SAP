import requests


def get_info_from_pypi(package_name):
    url = f"https://pypi.org/pypi/{package_name}/json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        package_info = data["info"]
        this_info = {'name': '', 'license': '', 'home_page': '', 'author': '', 'version': ''}
        this_info['name'] = package_info["name"] if package_info["name"] is not None else ''
        this_info['license'] = package_info["license"] if package_info["license"] is not None else ''
        this_info['home_page'] = package_info["home_page"].replace(' ', '') if package_info["home_page"] is not None else ''
        this_info['author'] = package_info["author"] if package_info["author"] is not None else ''

        return this_info
    else:
        return None


# Example usage
# package_names = ["wheel","yarg","docopt"]
lines = []
while True:
    line = input()
    if line:
        lines.append(line)
    else:
        break
package_names = lines
for package_name in package_names:
    result = get_info_from_pypi(package_name)
    print(result, ',')
