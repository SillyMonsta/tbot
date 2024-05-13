
def write(data, name):
    with open(name, 'a') as f:
        f.write(data + '\n')
        return ()


def read(name, num_lines):
    with open(name, 'r') as file:
        lines = file.readlines()
        last_lines = lines[-int(num_lines):]
    return last_lines

