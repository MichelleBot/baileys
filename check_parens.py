
def check_parentheses(file_path):
    stack = []
    with open(file_path, 'r') as f:
        for i, line in enumerate(f):
            for j, char in enumerate(line):
                if char == '(':
                    stack.append((i + 1, j + 1))
                elif char == ')':
                    if not stack:
                        print(f"Unbalanced ')' at line {i + 1}, column {j + 1}")
                    else:
                        stack.pop()
    for line, col in stack:
        print(f"Unbalanced '(' at line {line}, column {col}")

check_parentheses('lib/Socket/messages-send.js')
