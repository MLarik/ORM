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
        self.__created = None
        self.__updated = None

    def __getattr__(self, name):
        if name in ('_Entity__cursor',
                    '_Entity__fields',
                    '_Entity__loaded',
                    '_Entity__modified',
                    '_Entity__table'):
            raise AttributeError()
        if self.__modified:
            raise ModifiedError()
        if not self.__loaded:
            self.__load()

        value = self.__fields.get(name)

        return value

    def __setattr__(self, name, value):
        if not (hasattr(self, '_Entity__table')) or name in \
                ('_Entity__cursor',
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
            return self.__cursor.fetchone()
        except Exception:
            return None

    def __insert(self):
        gap = ", "
        keys = self.__fields.keys()
        values = self.__fields.values()
        placeholders = []
        for i in values:
            placeholders.append(f"'{i}'")

        self.__id = self.__execute_query(Entity.__insert_query.format(table=self.__table,
                                                                      columns=gap.join(keys),
                                                                      placeholders=gap.join(placeholders)))[0]
        self.__created = self.__execute_query(Entity.__select_query2.format(columns=f"{self.__table}_created", table=self.__table), (self.__id,))[0]
        self.__updated = self.__execute_query(Entity.__select_query2.format(columns=f"{self.__table}_updated", table=self.__table), (self.__id,))[0]

    def __load(self):
        if not self.__loaded and not(self.__id is None):
            values = self.__execute_query(Entity.__select_query.format(table=self.__table), (self.__id,))
            self.__created = values[f'{self.__table}_created']
            self.__updated = values[f'{self.__table}_updated']

            self.__fields.update(values)
            self.__loaded = True

    def __update(self):
        gap = ', '
        keys = list(self.__fields.keys())
        values = list(self.__fields.values())
        columns = []

        i = 0
        while i < len(keys):
            columns.append(f"{keys[i]}='{values[i]}'")
            i += 1

        self.__execute_query(Entity.__update_query.format(table=self.__table, columns=gap.join(columns)), (self.__id,))
        self.__updated = self.__execute_query(Entity.__select_query2.format(columns=f"{self.__table}_updated", table=self.__table), (self.__id,))[0]

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
        if not self.__loaded:
            self.__load()
        return self.__id

    @property
    def created(self):
        if self.__id is None:
            raise DatabaseError()
        if not self.__loaded:
            self.__load()
        return self.__created

    @property
    def updated(self):
        if not self.__loaded:
            self.__load()
        return self.__updated

    def save(self):
        if self.__id is None:
            self.__insert()
            self.__modified = False
        else:
            self.__update()
            self.__modified = False


# class Section(Entity):
#     _columns = ['title']
