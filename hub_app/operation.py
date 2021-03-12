class Operation:
    def __init__(self):
        self.operation_id = ''
        self.operation_class = ''
        self.operation_type = ''
        self.operation_tags = {}

    def set_params(self, op_id, op_class, op_type):
        self.operation_id = op_id
        self.operation_class = op_class.strip().lower()
        self.operation_type = op_type.strip().lower()

    def add_tag(self, tag_name, tag_value):
        t_val = tag_value
        if t_val:
            t_val = t_val.strip()
        self.operation_tags[tag_name.strip().lower()] = t_val

    def toJson(self):
        return {
            "operation_id": self.operation_id,
            "operation_class": self.operation_class,
            "operation_type": self.operation_type,
            "operation_tags": self.operation_tags
        }

    def toString(self):
        return str(self.toJson())

    def __str__(self):
        return self.toString()
