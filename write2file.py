
def write(data, name):
    with open(name, 'a') as f:
        f.write(data + '\n')
        return ()
