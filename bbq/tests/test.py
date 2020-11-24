import bbq.proto as proto

if __name__ == '__main__':
    addr = proto.AddressBook()
    addr.people.append('abc')
    s = addr.SerializeToString()
    print('len=', len(s))
