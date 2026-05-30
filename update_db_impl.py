
import os

file_path = 'core/src/db_impl.cc'

with open(file_path, 'r') as f:
    lines = f.readlines()

new_lines = []
skip = False
replaced_conflict = False
added_brace = False

for i, line in enumerate(lines):
    if 'if (find_write_conflict(key, check_ts, &conflict_ts)) {' in line:
        # Found the conflict check block start
        indent = line[:line.find('if')]
        new_lines.append(indent + 'int conflict_type = find_write_conflict(key, check_ts, &conflict_ts);\n')
        new_lines.append(indent + 'if (conflict_type == 1) {\n')
        new_lines.append(indent + '    return Status::Aborted("Write conflict");\n')
        new_lines.append(indent + '}\n')
        new_lines.append(indent + 'if (conflict_type == 0) {\n')
        skip = True
        replaced_conflict = True
        continue
    
    if skip:
        if 'return Status::Aborted("Write conflict");' in line:
            continue
        if line.strip() == '}':
            skip = False
            continue
        # Should not happen if block is exactly 3 lines
        
    if '// 4. Return Value' in line:
        # Need to close the 'else' block
        # The previous '}' closed the 'if (conflict_type == 0)' block
        # So we need one more '}'
        indent = line[:line.find('//')]
        new_lines.append(indent + '}\n')
        added_brace = True
        
    new_lines.append(line)

with open(file_path, 'w') as f:
    f.writelines(new_lines)

if replaced_conflict and added_brace:
    print("Successfully updated db_impl.cc")
else:
    print(f"Update failed: replaced_conflict={replaced_conflict}, added_brace={added_brace}")
