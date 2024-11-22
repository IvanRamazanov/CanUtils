from pathlib import Path


class Message:
    def __init__(self, text_data: str):
        msg_info = ''.join(text_data.split(':'))
        msg_info = msg_info.split(' ')

        self.id = int(msg_info[1])
        self.name = msg_info[2]
        self.dlc = int(msg_info[3])

        self.signals: list[Signal] = []

        # get pgn
        self.pgn = self.get_pgn(self.id)[0]

    def __str__(self):
        out = f'{Database.KW_OBJ} {self.id} {self.name}: {self.dlc} {Database.KW_DUMMY}'
        for sig in self.signals:
            out += f'\n {sig}'
        out += '\n'
        return out

    def add_sig(self, signal):
        self.signals.append(signal)

    def get_signal(self, sig_name):
        for sig in self.signals:
            if sig.name == sig_name:
                return sig
        return None

    @staticmethod
    def is_pdu1(msg_id: int) -> bool:
        msg_id = msg_id >> 16
        msg_id = msg_id & 0xFF
        return msg_id < 240

    @staticmethod
    def get_pgn(msg_id: int) -> (int, int):
        # check if Standard CAN
        is_standard_can = (msg_id & (1 << 31)) == 0

        if is_standard_can:
            # get as is?
            pgn = msg_id
            sa = 0xFE
        else:
            # remove SA
            sa = msg_id & 0xFF
            msg_id = msg_id >> 8
            # get PGN
            pgn = msg_id & 0x1FFFFF
        return pgn, sa


class Signal:
    def __init__(self, text_data: str):
        self.value_table: ValueTable | None = None
        self.multiplex = None
        # remove KW prefix
        text_data = text_data[len(Database.KW_SIG)+1:]

        # get name
        self.name = text_data[:text_data.find(' ')]
        text_data = text_data[len(self.name)+1:].strip()
        if text_data.startswith(':'):
            text_data = text_data[1:].strip()
        else:
            if text_data.startswith('m') or text_data.startswith('M'):
                # multiplexer setting
                self.multiplex = text_data[:text_data.find(' ')]
                text_data = text_data[len(self.multiplex) + 1:]

            # check again
            if not text_data.startswith(':'):
                raise Exception(f'Unknown signal format in {self.name}: expected ":", found nothing')

            # remove : delimiter
            text_data = text_data[1:].strip()

        # get start bit data
        self.start_bit = text_data[:text_data.find('|')]
        text_data = text_data[len(self.start_bit)+1:]
        self.start_bit = int(self.start_bit)

        # get bit length
        self.length = text_data[:text_data.find('@')]
        text_data = text_data[len(self.length) + 1:]
        self.length = int(self.length)

        # get bit format
        # 0 - motorola (reverse bit order)
        # 1 - Intel (direct bit order)
        # "-" - signed
        # "+" - unsigned
        format = text_data[:text_data.find(' ')]
        text_data = text_data[len(format) + 1:]
        if format[1] == '+':
            self.bit_signed = False
        elif format[1] == '-':
            self.bit_signed = True
        else:
            raise Exception(f'Bad bit format info in {self.name}. Expected + or -, got {format[1]}')

        if format[0] == '0':
            # Motorola
            self.bit_reverse = True
        elif format[0] == '1':
            # Intel
            self.bit_reverse = False
        else:
            raise Exception(f'Bad bit format info in {self.name}. Expected 0 or 1, got {format[0]}')

        # get factor and offset
        info = text_data[:text_data.find(') ')]
        if info.startswith('('):
            text_data = text_data[len(info) + 2:]
            info = info[1:]
        else:
            raise Exception(f'Bad signal formatting in {self.name}: expected "(" before factor,offset values')
        info = info.split(',')
        self.factor = self.__to_number(info[0])
        self.offset = self.__to_number(info[1])

        # get min max
        info = text_data[:text_data.find('] ')]
        if info.startswith('['):
            text_data = text_data[len(info) + 2:]
            info = info[1:]
        else:
            raise Exception(f'Bad signal formatting in {self.name}: expected "[" before min|max values')
        info = info.split('|')
        self.min_val = self.__to_number(info[0])
        self.max_val = self.__to_number(info[1])

        # get units
        info = text_data.split('"')
        self.units = info[1]

    def __str__(self):
        if self.multiplex is None:
            mul = ''
        else:
            mul = self.multiplex + ' '

        # format
        if self.bit_reverse:
            format = '0'
        else:
            format = '1'
        if self.bit_signed:
            format += '-'
        else:
            format += '+'

        return f'{Database.KW_SIG} {self.name} {mul}: {self.start_bit}|{self.length}@{format} ({self.factor},{self.offset}) [{self.min_val}|{self.max_val}] "{self.units}" {Database.KW_DUMMY}'

    @ staticmethod
    def __to_number(string):
        if string.find('.') != -1 or string.find('E') != 1 or string.find('e') != 1:
            # has dot, therefore float
            return float(string)
        else:
            return int(string)

    def bytes2data(self, raw_value: list):
        """
        converst raw message frame into signal value
        :param raw_value: list of uint8
        :return:
        """
        # assume that list goes from 0 to N and

        # extract
        byte_pos = self.start_bit // 8
        start_sub_pos = self.start_bit % 8
        byte_pos_end = (self.start_bit + self.length - 1) // 8
        raw_value = raw_value[byte_pos:byte_pos_end + 1]
        if self.bit_reverse:
            # reverse bit order
            # TODO reverse by bytes or bits??? are they already revresed??
            byte_order = 'big'
            raise Exception('Motorola is unsupported')
        else:
            byte_order = 'little'
        # note: handle sign later
        value = int.from_bytes(raw_value, byteorder=byte_order, signed=False)
        value = value >> start_sub_pos
        mask = 0
        for i in range(self.length):
            mask = (mask << 1) + 1
        value = value & mask

        if self.bit_signed:
            # -1 * MSB + rest
            msb = (1 << self.length) & value
            value = value & (mask >> 1)
            value = -1*msb + value
        else:
            # get as is
            pass

        # offsets and factor
        value *= self.factor
        value += self.offset

        return value


class ValueTable:
    def __init__(self, raw_text):
        self.msg_id = raw_text[:raw_text.find(' ')]
        raw_text = raw_text[len(self.msg_id)+1:]
        self.msg_id = int(self.msg_id)

        self.signal_name = raw_text[:raw_text.find(' ')]
        raw_text = raw_text[len(self.signal_name) + 1:]

        if raw_text.endswith(';'):
            raw_text = raw_text[:-1].strip()
        else:
            raise Exception(f'Bad Val Table format in {self.msg_id}.{self.signal_name}: should end with ";"')

        self.table: dict[int, str] = dict()
        while len(raw_text) > 0:
            table_val = raw_text[:raw_text.find(' ')]
            raw_text = raw_text[len(table_val) + 1:]
            table_val = int(table_val)

            if raw_text.startswith('"'):
                raw_text = raw_text[1:]
            else:
                raise Exception(f'Bad Val Table format in {self.msg_id}.{self.signal_name}: table name should start with "')
            table_name = raw_text[:raw_text.find('"')]
            raw_text = raw_text[len(table_name) + 2:]

            self.table[table_val] = table_name


class _Parser:
    def __init__(self, file):
        p = Path(file)
        with p.open('rb') as f:
            raw_data = f.read()
            try:
                self.lines = raw_data.decode('utf-8')
            except UnicodeDecodeError:
                self.lines = raw_data.decode('ansi')

            self.lines = self.lines.split('\n')

            # cleanup
            max_len = len(self.lines)
            for i in range(max_len):
                self.lines[i] = self.lines[i].replace('\t', ' ').strip()

            # join string values
            i = 0
            while i < max_len:
                if self.__is_odd(self.lines[i].count('"')):
                    # has unclosed string value
                    j = i + 1
                    while j < max_len:
                        self.lines[i] += self.lines[j]
                        if self.__is_odd(self.lines[j].count('"')):
                            # found the end of string
                            break
                        j += 1

                    # remove merged lines
                    while j - i > 0:
                        del self.lines[j]
                        j -= 1
                        max_len -= 1
                i += 1

            # remove empty lines
            #self.lines = [x for x in self.lines if len(x) > 0]
        self.index = 0
        self.length = len(self.lines)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == self.length:
            raise StopIteration

        out = self.lines[self.index]
        self.index += 1
        return out

    def __getitem__(self, item):
        return self.lines[item]

    @staticmethod
    def __is_odd(val) -> bool:
        return val % 2 == 1


class Database:
    KW_OBJ = 'BO_'
    KW_SIG = 'SG_'
    KW_DUMMY = 'Vector__XXX'
    KW_NAME_SPACE = 'NS_'
    KW_BAUDRATE = 'BS_'
    KW_NODES = 'BU_'
    KW_VERSION = 'VERSION '
    KW_UNUSED_VAL_TABLE = 'VAL_TABLE_'
    MSG_UNUSED = 'VECTOR__INDEPENDENT_SIG_MSG'
    KW_COMMENT = 'CM_'
    KW_ATTR_DEFINE = 'BA_DEF_ '
    KW_ATTR_DEF_VAL = 'BA_DEF_DEF_ '
    KW_ATTR_VAL = 'BA_ '
    KW_SIG_VAL_TABLE = 'VAL_ '

    def __init__(self, file):
        self.version = None
        self.net_nodes = None
        self.messages: list[Message] = []
        self.name_space = Database.NameSpace()
        self.unused_val_tables = []
        self.comments = []
        self.defines = []
        self.etc = []

        parser = _Parser(file)
        for line in parser:
            if line.startswith(Database.KW_NAME_SPACE):
                while True:
                    n_line = next(parser)
                    if len(n_line) == 0:
                        break
                    self.name_space.add(n_line)
            elif line.startswith(Database.KW_VERSION):
                self.version = line[len(Database.KW_VERSION):]
                # TODO extract value
            elif line.startswith(Database.KW_BAUDRATE):
                # legacy entry, ignore
                pass
            elif line.startswith(Database.KW_NODES):
                # TODO parse nodes
                self.net_nodes = line
            elif line.startswith(Database.KW_UNUSED_VAL_TABLE):
                self.unused_val_tables.append(line)
            elif line.startswith(Database.KW_OBJ):
                # message
                msg = Message(line)

                # parse signals
                while True:
                    n_line = next(parser)
                    if len(n_line) == 0:
                        break
                    msg.add_sig(Signal(n_line))

                self.messages.append(msg)
            elif line.startswith(Database.KW_COMMENT):
                self.comments.append(line)
            elif line.startswith(Database.KW_ATTR_DEFINE):
                self.defines.append(Database.Attribute(text_line=line[len(Database.KW_ATTR_DEFINE):]))
            elif line.startswith(Database.KW_ATTR_DEF_VAL):
                def_val = Database.Attribute.DefaultValue(line[len(Database.KW_ATTR_DEF_VAL):])
                # find owner
                for attr in self.defines:
                    if attr.name == def_val.owner:
                        attr.set_default_value(def_val)
                        break
            elif line.startswith(Database.KW_ATTR_VAL):
                val_setter = Database.Attribute.ValueSetter(line[len(Database.KW_ATTR_VAL):])
                # find owner
                for attr in self.defines:
                    if attr.name == val_setter.owner:
                        attr.add_value(val_setter)
                        break
            elif line.startswith(Database.KW_SIG_VAL_TABLE):
                # TODO parse and pass into signal
                vt_sig = ValueTable(line[len(Database.KW_SIG_VAL_TABLE):])
                for msg in self.messages:
                    if msg.id == vt_sig.msg_id:
                        for sig in msg.signals:
                            if sig.name == vt_sig.signal_name:
                                sig.value_table = vt_sig
                                break
                        else:
                            raise Exception(f"Can't find signal for value table: {line}")
                        break
                else:
                    raise Exception(f"Can't find message for value table: {line}")
                self.etc.append(line)
            else:
                if len(line) > 0:
                    self.etc.append(line)

    def __str__(self):
        out = f'{Database.KW_VERSION} {self.version}\n\n'
        out += f'{self.name_space}\n'

        # legacy
        out += f'{Database.KW_BAUDRATE}:\n'

        # nodes
        out += f'{self.net_nodes}\n'

        # unused tables
        out += '\n'.join(self.unused_val_tables) + '\n\n'

        # messages
        out += '\n\n'.join((str(x) for x in self.messages)) + '\n\n'

        # comments
        for comm in self.comments:
            out += comm + '\n'

        # defines
        for define in self.defines:
            out += str(define) + '\n'
        for define in self.defines:
            out += str(define.default_val) + '\n'
        for define in self.defines:
            for val in define.value_setters:
                out += str(val) + '\n'

        # TODO value tables

        # etc
        out += '\n'.join(self.etc)

        return out

    def to_file(self, path):
        str_out = str(self).encode('utf-8')
        with path.open('wb') as f:
            f.write(str_out)

    def get_attribute(self, name):
        for a in self.defines:
            if a.name == name:
                return a
        return None

    def get_message(self, msg_name):
        for m in self.messages:
            if m.name == msg_name:
                return m
        return None

    def add_attribute(self, name, val_type, value):
        a_val = Database.Attribute(name=name, val_type=val_type)
        def_val = Database.Attribute.DefaultValue(owner=name, value=value)
        a_val.set_default_value(def_val)
        self.defines.append(a_val)

    def merge(self, other_database):
        # TODO ?
        # self.net_nodes = None

        self.name_space.merge(other_database.name_space)

        self.messages += other_database.messages
        self.unused_val_tables += other_database.unused_val_tables
        self.comments += other_database.comments
        self.defines += other_database.defines
        self.etc += other_database.etc

    class NameSpace:
        def __init__(self):
            self.items = []

        def __str__(self):
            out = f'{Database.KW_NAME_SPACE} :'
            for elem in self.items:
                out += f'\n\t{elem}'
            out += '\n'
            return out

        def add(self, val):
            self.items.append(val)

        def merge(self, other_ns):
            for ns_i in other_ns.items:
                if ns_i not in self.items:
                    self.add(ns_i)

    class Attribute:
        def __init__(self, text_line: str = None, name=None, val_type=None):
            if text_line is not None:
                if not (text_line.startswith(' ') or text_line.startswith('"')):
                    # has type
                    self.owner_type = text_line[:text_line.find(' ')]
                    text_line = text_line[len(self.owner_type)+1:].strip()
                else:
                    # no type
                    self.owner_type = None
                    text_line = text_line.strip()

                # get name
                if text_line.startswith('"'):
                    text_line = text_line[1:]
                else:
                    raise Exception('Wrong format in ' + text_line)
                self.name = text_line[:text_line.find('"')]
                text_line = text_line[len(self.name) + 2:]

                # get val type
                if text_line.endswith(';'):
                    text_line = text_line[:-1]
                else:
                    raise Exception('Wrong format in ' + text_line)
                self.value_type = text_line
            else:
                self.name = name
                self.value_type = val_type
                self.owner_type = None

            # will be inited later
            self.default_val = None
            self.value_setters = []

        def __str__(self):
            if self.owner_type is None:
                type = ''
            else:
                type = self.owner_type + ' '
            return f'{Database.KW_ATTR_DEFINE}{type} "{self.name}" {self.value_type};'

        def set_default_value(self, value):
            if self.default_val is not None:
                raise Exception(f'Attribute {self.name} already has default value')

            self.default_val = value

        def add_value(self, val_setter):
            self.value_setters.append(val_setter)

        def to_string_default_val(self):
            return str(self.default_val)

        def to_string_values(self):
            return '\n'.join((str(x) for x in self.value_setters))

        class DefaultValue:
            def __init__(self, data: str = None, owner: str = None, value=None):
                if data is not None:
                    data = data.strip()
                    if data.endswith(';'):
                        data = data[:-1]
                    else:
                        raise Exception('Wrong format in ' + data)
                    if data.startswith('"'):
                        data = data[1:]
                    else:
                        raise Exception('Wrong format in ' + data)
                    self.owner = data[:data.find('"')]
                    data = data[len(self.owner)+2:]

                    self.value = data
                else:
                    self.owner = owner
                    self.value = value

            def __str__(self):
                return f'{Database.KW_ATTR_DEF_VAL} "{self.owner}" {self.value};'

        class ValueSetter:
            def __init__(self, data: str):
                data = data.strip()
                if data.endswith(';'):
                    data = data[:-1]
                else:
                    raise Exception('Wrong format in ' + data)
                if data.startswith('"'):
                    data = data[1:]
                else:
                    raise Exception('Wrong format in ' + data)
                self.owner = data[:data.find('"')]
                data = data[len(self.owner)+2:]

                self.value = data

            def __str__(self):
                return f'{Database.KW_ATTR_VAL}"{self.owner}" {self.value};'
