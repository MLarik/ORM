import psycopg2.extras


class DatabaseError(Exception):
    pass


class NotFoundError(Exception):
    pass


class ModifiedError(Exception):
    pass


class Entity(object):
    db = None

    try:
        db = psycopg2.connect(user='mathew', password='root', dbname='shop', host='localhost', port='5433')
    except Exception:
        raise DatabaseError()

    __delete_query = 'DELETE FROM "{table}" WHERE {table}_id=%s'
    __insert_query = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) ' \
                     'RETURNING ' \
                     '"{table}_id", ' \
                     '"{table}_created"'
    __list_query = 'SELECT * FROM "{table}"'
    __select_query = 'SELECT * FROM "{table}" WHERE "{table}_id"=%s'
    __update_query = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s RETURNING "{table}_updated"'
    __select_query2 = 'SELECT {columns} FROM {table} WHERE {table}_id=%s'
    __columns_query = "SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"

    def __init__(self, id=None):
        if self.__class__.db is None:
            raise DatabaseError()

        self.__cursor = self.__class__.db.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )
        self.__fields = {}
        self.__id = id
        self.__loaded = False
        self.__modified = False
        self.__table = self.__class__.__name__.lower()
        self.__created = None
        self.__updated = None

    def __getattr__(self, name):
        if name in ('_Entity__id',
                    '_Entity__cursor',
                    '_Entity__fields',
                    '_Entity__loaded',
                    '_Entity__modified',
                    '_Entity__table',
                    '_Entity__created',
                    '_Entity__updated'):
            raise AttributeError()
        if self.__modified:
            raise ModifiedError()
        self.__load()

        value = self.__fields.get(name)

        return value

    def __setattr__(self, name, value):
        if name in ('_Entity__id',
                    '_Entity__cursor',
                    '_Entity__fields',
                    '_Entity__loaded',
                    '_Entity__modified',
                    '_Entity__table',
                    '_Entity__created',
                    '_Entity__updated'):
            super(Entity, self).__setattr__(name, value)
        else:
            self._set_column(name, value)
            self.__modified = True

    def __execute_query(self, query, args=None):
        try:
            if not (args is None):
                self.__cursor.execute(query, args)
            else:
                self.__cursor.execute(query)
        except Exception:
            Entity.db.rollback()
            raise DatabaseError()
        Entity.db.commit()
        # print(query)

        try:
            return self.__cursor.fetchall()[0]
        except Exception:
            return None

    def __insert(self):
        keys = self.__fields.keys()
        values = self.__fields.values()
        placeholders = []
        for i in values:
            placeholders.append(f"'{i}'")

        values = self.__execute_query(Entity.__insert_query.format(table=self.__table,
                                                                   columns=', '.join(keys),
                                                                   placeholders=', '.join(placeholders)))
        self.__id = values[f'{self.__table}_id']
        self.__created = values[f'{self.__table}_created']
        self.__updated = self.__created

    def __load(self):
        if not self.__loaded and not (self.__id is None):
            values = self.__execute_query(Entity.__select_query.format(table=self.__table), (self.__id,))

            self.__created = values[f'{self.__table}_created']
            self.__updated = values[f'{self.__table}_updated']

            self.__fields.update(values)
            self.__loaded = True

    def __update(self):
        keys = list(self.__fields.keys())
        values = list(self.__fields.values())
        columns = []

        i = 0
        while i < len(keys):
            columns.append(f"{keys[i]}='{values[i]}'")
            i += 1

        result = self.__execute_query(Entity.__update_query.format(table=self.__table, columns=', '.join(columns)),
                                      (self.__id,))

        self.__updated = result[f'{self.__table}_updated']

    def _get_column(self, name):
        return self.__fields[f'{self.__table}_{name}']

    def _set_column(self, name, value):
        self.__fields[name] = value

    @classmethod
    def all(cls):
        instances = []

        temp_cursor = cls.db.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )

        temp_cursor.execute(cls.__list_query.format(table=cls.__name__.lower()))
        data = temp_cursor.fetchall()
        temp_cursor.execute(cls.__columns_query.format(table=cls.__name__.lower()))
        keys = temp_cursor.fetchall()

        # this loop should write values ​​to instances of the class and should assign loaded = True
        i = 0

        while i < len(data):
            temp_inst = cls()
            temp_inst._Entity__loaded = True
            temp_fields = {}

            j = 0
            while j < len(data[i]):
                temp_fields[keys[j][0]] = data[i][j]

                j += 1
            temp_inst._Entity__fields.update(temp_fields)
            instances.append(temp_inst)

            i += 1

        return instances

    def delete(self):
        if self.__id is None:
            raise RuntimeError()
        else:
            self.__execute_query(Entity.__delete_query.format(table=self.__table), (self.__id,))
            self.__id = None

    @property
    def id(self):
        self.__load()
        return self.__id

    @property
    def created(self):
        if self.__id is None:
            raise DatabaseError()
        self.__load()
        return self.__created

    @property
    def updated(self):
        self.__load()
        return self.__updated

    def save(self):
        if self.__id is None:
            self.__insert()
            self.__modified = False
        else:
            self.__update()
            self.__modified = False


class Section(Entity):
    _columns = ['title']


if __name__ == "__main__":
    s = Section(155)

    for s in Section.all():
        print(s.section_title)
