package overwrite

import "core:flags"
import "core:fmt"
import "core:os"
import "core:strings"

TIMES :: 3
CHUNK := [4096]u8{}

Options :: struct {
	m: ^os.File `args:"file=r" usage:"File where each line is the path to another file."`,
	s: ^os.File `args:"file=w" usage:"File to be overwritten."`,
}

main :: proc() {
	opts: Options
	flags.parse_or_exit(&opts, os.args, .Odin)

	if opts.m != nil {
		if !many(opts.m) {
			fmt.println("failed to open input file")
		}
	} else if opts.s != nil {
		if !single(opts.s) {
			fmt.println("failed to overwrite file")
		}
	}
}

single :: proc(file: ^os.File) -> (ok: bool) {
	size := file_size(file) or_return
	for i in 0 ..< TIMES do overwrite(file, size) or_return
	return true
}

many :: proc(input_file: ^os.File) -> bool {
	buf, err := os.read_entire_file(input_file, context.allocator)
	if err != nil do return false
	defer delete(buf)

	text := transmute(string)(buf)
	for line in strings.split_lines_iterator(&text) {
		if len(line) == 0 do continue
		if file, size, ok := info(line); ok {
			for i in 0 ..< TIMES do overwrite(file, size) or_break
		} else do fmt.printfln("failed to open %v", line)
	}

	return true
}

file_size :: proc(file: ^os.File) -> (size: int, ok: bool) {
	file_info, err := os.fstat(file, context.allocator)
	if err != nil do return 0, false
	os.file_info_delete(file_info, context.allocator)
	return int(file_info.size), true
}

open :: proc(file_path: string) -> (^os.File, bool) {
	file, err := os.open(file_path, {.Write})
	if err != nil do return nil, false
	return file, true
}

info :: proc(file_path: string) -> (file: ^os.File, size: int, ok: bool) {
	file = open(file_path) or_return
	size = file_size(file) or_return
	return file, size, true
}

overwrite :: proc(file: ^os.File, size: int) -> bool {
	os.seek(file, 0, .Start)

	chunk := CHUNK[:]
	written := 0

	for written < size {
		remaining := size - written
		chunk_len := min(len(chunk), remaining)

		n, err := os.write(file, chunk[:chunk_len])
		if err != nil do break

		written += n
	}

	return true
}

