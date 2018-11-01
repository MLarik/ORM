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
    __insert_query = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING "{table}_id"'
    __list_query = 'SELECT * FROM "{table}"'
    __select_query = 'SELECT * FROM "{table}" WHERE "{table}_id"=%s'
    __update_query = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'
    __select_query2 = 'SELECT {columns} FROM {table} WHERE {table}_id=%s'
    __count_query = 'SELECT COUNT(*) FROM "{table}"'

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

    def __getattr__(self, name):
        if name in ('_Entity__cursor', '_Entity__fields', '_Entity__loaded', '_Entity__modified', '_Entity__table'):
            raise AttributeError()
        if self.__modified:
            raise ModifiedError()
        if not self.__loaded:
            self.__load()

        value = self.__fields.get(name)
        if value is None:
            raise AttributeError()
        return value

    def __setattr__(self, name, value):
        if not (hasattr(self, '_Entity__table')) or name in \
                ('_Entity__cursor',
                 '_Entity__fields',
                 '_Entity__loaded',
                 '_Entity__modified',
                 '_Entity__table',):
            self.__dict__[name] = value
        else:
            self._set_column(name, value)
            self.__modified = True

    def __execute_query(self, query, args=None):
        try:
            if not(args is None):
                self.__cursor.execute(query, args)
            else:
                self.__cursor.execute(query)
        except Exception:
            Entity.db.rollback()
            raise DatabaseError()
        Entity.db.commit()
        # print(query)

        try:
           fetch = self.__cursor.fetchone()
        except Exception:
            return None
        return fetch

    def __insert(self):
        if self.__id is None:
            gap = ", "
            keys = self.__fields.keys()
            values = self.__fields.values()
            values2 = []
            for i in values:
                values2.append(f"'{i}'")
            self.__id = self.__execute_query(Entity.__insert_query.format(table=self.__table,
                                                                          columns=gap.join(keys),
                                                                          placeholders=gap.join(values)))

    def __load(self):
        if not self.__loaded and not(self.__id is None):
            values = self.__execute_query(Entity.__select_query.format(table=self.__table), (self.__id,))

            self.__fields.update(values)
            self.__loaded = True

    def __update(self):  # !!!!!!!!!!!!!!
        if not (self.id is None):
            gap = ', '
            keys = list(self.__fields.keys())
            values = list(self.__fields.values())
            columns = []

            i = 0
            while i < len(keys):
                columns.append(f"{keys[i]}='{values[i]}'")
                i += 1

            self.__execute_query(Entity.__update_query.format(table=self.__table, columns=gap.join(columns)), (self.__id,))

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

        temp_cursor.execute(cls.__count_query.format(table=cls.__name__.lower()))
        limit = temp_cursor.fetchone()[0]

        i = 1
        while i <= limit:
            instances.append(cls(i))
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
        return self.__id

    @property
    def created(self):
        return self.__execute_query(Entity.__select_query2.format(columns=f"{self.__table}_created", table=self.__table), (self.__id,))[0]

    @property
    def updated(self):
        return self.__execute_query(Entity.__select_query2.format(columns=f"{self.__table}_updated", table=self.__table), (self.__id,))[0]

    def save(self):
        if self.__id is None:
            self.__insert()
            self.__modified = False
        else:
            self.__update()
            self.__modified = False


# class Section(Entity):
#     _columns = ['title']


