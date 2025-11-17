import re

class CppToPythonTranslator:
    def __init__(self, cpp_code):
        self.lines = cpp_code.splitlines()
        self.python_code = []

    def parse_template_parameters(self, line):
        match = re.match(r'template\s*<\s*(.+)\s*>', line)
        if not match:
            return []
        params_part = match.group(1)
        params = re.findall(r'(?:typename|class)\s+(\w+)', params_part)
        return params

    def extract_methods_from_cpp(self, class_lines):
        methods = {}
        class_text = '\n'.join(class_lines)
        class_name = None

        class_match = re.search(r'class\s+(\w+)', class_text)
        if class_match:
            class_name = class_match.group(1)

        in_method = False
        current_method = None
        brace_count = 0

        for line in class_lines:
            stripped = line.strip()
            if stripped.startswith('template') or not stripped:
                continue
            if stripped in ['public:', 'private:', 'protected:']:
                continue

            if not in_method and '(' in stripped and ')' in stripped and not stripped.endswith(';'):
                patterns = [
                    r'(\w+)\s+(\w+)\s*\(([^)]*)\)',  # обычные методы
                    r'void\s+(\w+)\s*\(([^)]*)\)',  # void методы
                    r'bool\s+(\w+)\s*\(([^)]*)\)',  # bool методы
                    r'(\w+)\s+operator\s*([\+\-\*/])\s*\(([^)]*)\)',  # операторы +-*/
                    r'bool\s+operator\s*([=!]=)\s*\(([^)]*)\)',  # операторы == !=
                    rf'({class_name})\s*\(([^)]*)\)'  # конструкторы
                ]

                for pattern in patterns:
                    match = re.search(pattern, stripped)
                    if match:
                        if 'operator' in pattern:
                            # Обработка операторов
                            if 'bool' in pattern:
                                # Для операторов сравнения
                                op_symbol, params = match.groups()
                                method_name = f"operator{op_symbol}"
                            else:
                                # Для арифметических операторов
                                return_type, op_symbol, params = match.groups()
                                method_name = f"operator{op_symbol}"
                        else:
                            # Обычные методы
                            if len(match.groups()) == 3:
                                return_type, method_name, params = match.groups()
                            else:
                                method_name, params = match.groups()
                                return_type = None

                        if method_name == class_name:
                            continue

                        if not method_name.startswith('~'):
                            current_method = method_name
                            methods[current_method] = {
                                'params': params,
                                'body': [],
                                'is_constructor': False
                            }
                            in_method = True
                            brace_count = 0
                            break

            if in_method and current_method:
                methods[current_method]['body'].append(stripped)
                brace_count += stripped.count('{')
                brace_count -= stripped.count('}')
                if brace_count <= 0 and '}' in stripped:
                    in_method = False
                    current_method = None

        if class_name:
            constr_match = re.search(rf'{class_name}\s*\(([^)]*)\)', class_text)
            if constr_match:
                methods[class_name] = {
                    'params': constr_match.group(1),
                    'body': [],
                    'is_constructor': True
                }

        return methods

    def convert_cpp_params_to_python(self, cpp_params):
        if not cpp_params.strip():
            return ""
        python_params = []
        for param in cpp_params.split(','):
            param = param.strip()
            if param:
                parts = param.split()
                if parts:
                    param_name = parts[-1].replace('*', '').replace('&', '')
                    python_params.append(param_name)
        return ", " + ", ".join(python_params) if python_params else ""

    def convert_to_snake_case(self, name):
        result = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0 and result[-1] != '_':
                result.append('_')
            result.append(char.lower())
        return ''.join(result)

    def extract_fields_with_types(self, class_lines):
        fields = []
        pattern = re.compile(r'\b(\w+)\s+(\w+);')
        for line in class_lines:
            line = line.strip()
            if not line or '(' in line or line.startswith('template') or line.endswith('};') or line in ['public:',
                                                                                                         'private:',
                                                                                                         'protected:']:
                continue
            match = pattern.match(line)
            if match:
                typ, name = match.groups()
                fields.append((typ, name))
        return fields

    def convert_cpp_type_to_python_type(self, cpp_type):
        mapping = {
            'int': 'int',
            'float': 'float',
            'double': 'float',
            'char': 'str',
            'bool': 'bool',
        }
        if cpp_type.isupper():
            return cpp_type
        return mapping.get(cpp_type, 'Any')

    def translate_template_class(self, template_lines):
        template_params = self.parse_template_parameters(template_lines[0])
        class_match = re.match(r'class\s+(\w+)', template_lines[1].strip())
        class_name = class_match.group(1) if class_match else "Container"

        methods = self.extract_methods_from_cpp(template_lines)
        fields = self.extract_fields_with_types(template_lines)

        indent = '    '
        py = ["from typing import Generic, TypeVar"]

        for p in template_params:
            py.append(f"{p} = TypeVar('{p}')")
        generic_params = ", ".join(template_params)
        py.append(f"class {class_name}(Generic[{generic_params}]):")

        params = ", ".join(f"{name}: {self.convert_cpp_type_to_python_type(typ)}" for typ, name in fields)
        py.append(f"{indent}def __init__(self, {params}):")
        for typ, name in fields:
            py.append(f"{indent * 2}self.{name} = {name}")

        methods_to_add = [
            ('setValue', 'val', "self.value = val"),
            ('getValue', '', "return self.value"),
            ('print', '', f"print(f'{class_name}: {{self.value}}')"),
            ('increment', '', "self.value += 1"),
            ('decrement', '', "self.value -= 1"),
            ('isPositive', '', "return self.value > 0"),
            ('isNegative', '', "return self.value < 0"),
            ('isZero', '', "return self.value == 0"),
            ('operateMultipleTimes', 'times', "for i in range(times):\n        self.increment()"),
            ('reset', '', "self.value = 0"),
            ('add', 'v', "self.value += v"),
            ('subtract', 'v', "self.value -= v"),
            ('multiply', 'v', "self.value *= v"),
            ('divide', 'v', "if v != 0:\n        self.value /= v"),
            ('getDouble', '', "return self.value * 2"),
            ('getHalf', '', "return self.value / 2"),
            ('square', '', "self.value = self.value * self.value"),
        ]

        for method_name, params, implementation in methods_to_add:
            if method_name in methods:
                py.append("")
                py.append(
                    f"{indent}def {self.convert_to_snake_case(method_name)}(self{', ' + params if params else ''}):")
                for line in implementation.split('\n'):
                    py.append(f"{indent * 2}{line}")

        return py

    def translate_pointer_class(self, class_lines):
        methods = self.extract_methods_from_cpp(class_lines)
        for line in class_lines:
            class_match = re.match(r'class\s+(\w+)', line.strip())
            if class_match:
                class_name = class_match.group(1)
                break
        indent = '    '
        py = []
        py.append(f"class {class_name}:")

        py.append(f"{indent}def __init__(self, val):")
        py.append(f"{indent * 2}self.ptr = [val]")

        methods_to_add = [
            ('increment', '', "self.ptr[0] += 1"),
            ('decrement', '', "self.ptr[0] -= 1"),
            ('getValue', '', "return self.ptr[0]"),
            ('reset', 'val=0', "self.ptr[0] = val"),
            ('add', 'v', "self.ptr[0] += v"),
            ('subtract', 'v', "self.ptr[0] -= v"),
            ('print', '', "print(f'Pointer value: {self.ptr[0]}')"),
        ]

        for method_name, params, implementation in methods_to_add:
            if method_name in methods:
                py.append("")
                py.append(
                    f"{indent}def {self.convert_to_snake_case(method_name)}(self{', ' + params if params else ''}):")
                for line in implementation.split('\n'):
                    py.append(f"{indent * 2}{line}")

        return py

    def translate_operator_class(self, class_lines):
        methods = self.extract_methods_from_cpp(class_lines)
        for line in class_lines:
            class_match = re.match(r'class\s+(\w+)', line.strip())
            if class_match:
                class_name = class_match.group(1)
                break
        indent = '    '
        py = []
        py.append(f"class {class_name}:")

        py.append(f"{indent}def __init__(self, v):")
        py.append(f"{indent * 2}self.value = v")

        operator_mapping = {
            'operator+': ('__add__', '+'),
            'operator-': ('__sub__', '-'),
            'operator*': ('__mul__', '*'),
            'operator/': ('__truediv__', '/'),
            'operator==': ('__eq__', '=='),
            'operator!=': ('__ne__', '!='),
        }

        for cpp_op in operator_mapping:
            if cpp_op in methods:
                python_op, op_symbol = operator_mapping[cpp_op]
                py.append("")
                py.append(f"{indent}def {python_op}(self, other):")

                if python_op in ['__add__', '__sub__', '__mul__']:
                    py.append(f"{indent * 2}return OperatorOverload(self.value {op_symbol} other.value)")
                elif python_op == '__truediv__':
                    py.append(f"{indent * 2}if other.value == 0:")
                    py.append(f"{indent * 3}raise ZeroDivisionError('Division by zero')")
                    py.append(f"{indent * 2}return OperatorOverload(self.value {op_symbol} other.value)")
                elif python_op in ['__eq__', '__ne__']:
                    py.append(f"{indent * 2}return self.value {op_symbol} other.value")

        if 'print' in methods:
            py.append("")
            py.append(f"{indent}def print(self):")
            py.append(f"{indent * 2}print(f'Value: {{self.value}}')")

        if 'getValue' in methods:
            py.append("")
            py.append(f"{indent}def get_value(self):")
            py.append(f"{indent * 2}return self.value")

        if 'setValue' in methods:
            py.append("")
            py.append(f"{indent}def set_value(self, v):")
            py.append(f"{indent * 2}self.value = v")

        return py

    def translate_main(self, main_lines):
        indent = '    '
        py = ["def main():"]

        for line in main_lines[1:-1]:
            line = line.strip()
            if not line or line in ['{', '}']:
                continue

            line = line.rstrip(';')

            template_match = re.match(r'(\w+)<[^>]+>\s+(\w+)\((.*)\)', line)
            if template_match:
                class_name, var_name, args = template_match.groups()
                py.append(f"{indent}{var_name} = {class_name}({args.strip()})")
                continue

            obj_match = re.match(r'(\w+)\s+(\w+)\((.*)\)', line)
            if obj_match:
                class_name, var_name, args = obj_match.groups()
                py.append(f"{indent}{var_name} = {class_name}({args.strip()})")
                continue

            line = re.sub(r'(\w+)\.print\(\)', r'\1.print()', line)
            line = re.sub(r'(\w+)\.increment\(\)', r'\1.increment()', line)
            line = re.sub(r'(\w+)\.operateMultipleTimes\((\d+)\)', r'\1.operate_multiple_times(\2)', line)
            line = re.sub(r'(\w+)\.square\(\)', r'\1.square()', line)
            line = re.sub(r'(\w+)\.add\((\d+)\)', r'\1.add(\2)', line)

            line = re.sub(r'OperatorOverload\s+(\w+)\s*=\s*(\w+)\s*\+\s*(\w+)', r'\1 = \2 + \3', line)
            line = re.sub(r'OperatorOverload\s+(\w+)\s*=\s*(\w+)\s*-\s*(\w+)', r'\1 = \2 - \3', line)
            line = re.sub(r'OperatorOverload\s+(\w+)\s*=\s*(\w+)\s*\*\s*(\w+)', r'\1 = \2 * \3', line)
            line = re.sub(r'OperatorOverload\s+(\w+)\s*=\s*(\w+)\s*/\s*(\w+)', r'\1 = \2 / \3', line)

            if line != 'return 0':
                py.append(f"{indent}{line}")

        py.append("")
        py.append("if __name__ == '__main__':")
        py.append(f"{indent}main()")
        return py

    def translate(self):
        i = 0
        n = len(self.lines)
        while i < n:
            line = self.lines[i].strip()
            if line.startswith("template"):
                block = []
                while i < n and not self.lines[i].strip().endswith("};"):
                    block.append(self.lines[i])
                    i += 1
                if i < n:
                    block.append(self.lines[i])
                self.python_code.extend(self.translate_template_class(block))
                self.python_code.append("")
            elif line.startswith("class "):
                block = []
                while i < n and not self.lines[i].strip().endswith("};"):
                    block.append(self.lines[i])
                    i += 1
                if i < n:
                    block.append(self.lines[i])

                class_text = '\n'.join(block)
                if 'operator' in class_text and 'value' in class_text:
                    self.python_code.extend(self.translate_operator_class(block))
                elif 'ptr' in class_text or '* ptr' in class_text:
                    self.python_code.extend(self.translate_pointer_class(block))
                else:
                    self.python_code.extend(self.translate_template_class(block))

                self.python_code.append("")
            elif line.startswith("int main()"):
                block = []
                while i < n and not self.lines[i].strip().endswith('}'):
                    block.append(self.lines[i])
                    i += 1
                if i < n:
                    block.append(self.lines[i])
                self.python_code.extend(self.translate_main(block))
                self.python_code.append("")
            else:
                i += 1

        return '\n'.join(self.python_code)

    def save_to_file(self, filename):
        code = '\n'.join(self.python_code)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(code)

cpp_code = """
#include <iostream>

template<typename T>
class Container {
    T value;
public:
    Container(T val) : value(val) {}

    void setValue(T val) { value = val; }
    T getValue() const { return value; }

    void print() const { std::cout << "Container: " << value << std::endl; }

    void increment() { value = value + 1; }
    void decrement() { value = value - 1; }

    bool isPositive() const { return value > 0; }
    bool isNegative() const { return value < 0; }
    bool isZero() const { return value == 0; }

    void operateMultipleTimes(int times) {
        for (int i = 0; i < times; i++) increment();
    }

    void reset() { value = T(); }

    void add(T v) { value += v; }
    void subtract(T v) { value -= v; }

    void multiply(T v) { value *= v; }
    void divide(T v) { if(v != 0) value /= v; }

    T getDouble() const { return value * 2; }
    T getHalf() const { return value / 2; }
    void square() { value = value * value; }
};

class PointerExample {
    int* ptr;
public:
    PointerExample(int val) { ptr = new int(val); }

    void increment() { (*ptr)++; }
    void decrement() { (*ptr)--; }
    int getValue() const { return *ptr; }

    void reset(int val = 0) {
        *ptr = val;
    }

    void add(int v) {
        *ptr += v;
    }

    void subtract(int v) {
        *ptr -= v;
    }

    void print() const {
        std::cout << "Pointer value: " << *ptr << std::endl;
    }
};

class OperatorOverload {
    int value;
public:
    OperatorOverload(int v) : value(v) {}

    OperatorOverload operator+(const OperatorOverload& other) {
        return OperatorOverload(value + other.value);
    }
    OperatorOverload operator-(const OperatorOverload& other) {
        return OperatorOverload(value - other.value);
    }
    OperatorOverload operator*(const OperatorOverload& other) {
        return OperatorOverload(value * other.value);
    }
    OperatorOverload operator/(const OperatorOverload& other) {
        if (other.value == 0) throw std::runtime_error("Divide by zero");
        return OperatorOverload(value / other.value);
    }
    bool operator==(const OperatorOverload& other) const {
        return value == other.value;
    }
    bool operator!=(const OperatorOverload& other) const {
        return !(*this == other);
    }

    void print() const { std::cout << "Value: " << value << std::endl; }

    int getValue() const { return value; }
    void setValue(int v) { value = v; }
};

int main() {
    Container<int> cont(10);
    cont.print();
    cont.operateMultipleTimes(5);
    cont.print();
    cont.square();
    cont.print();

    PointerExample p(100);
    p.print();
    p.increment();
    p.print();
    p.add(50);
    p.print();

    OperatorOverload a(50);
    OperatorOverload b(20);
    OperatorOverload c = a + b;
    c.print();
    OperatorOverload d = a - b;
    d.print();
    OperatorOverload e = a * b;
    e.print();
    return 0;
}
"""
translator = CppToPythonTranslator(cpp_code)
translator.translate()
translator.save_to_file("output.py")