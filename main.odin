package overwrite

import "async:."
import "async:io"
import "core:flags"
import "core:fmt"
import "core:os"
import "core:strings"

CHUNK := [4096]u8{}

Options :: struct {
	many:      string `args:"name=m" usage:"file containing paths to process"`,
	single:    string `args:"name=s" usage:"single file or directory to process"`,
	explorers: int `args:"name=e" usage:"number of directory workers"`,
	writers:   int `args:"name=w" usage:"number of file workers"`,
	times:     int `args:"name=t" usage:"number of times to overwrite each file"`,
	delete:    bool `args:"name=d" usage:"delete files/dirs"`,
	verbose:   bool `args:"name=v" usage:"enable log messages"`,
}

File :: struct {
	path:   string,
	handle: io.Handle,
}

State :: struct {
	path_ch:      async.Chan(string),
	dir_ch:       async.Chan(string),
	file_ch:      async.Chan(File),
	delete_paths: [dynamic]string,
	//
	explorers:    int,
	writers:      int,
	times:        int,
	verbose:      bool,
	//
	wg:           async.Wait_Group,
}

main :: proc() {
	opts := Options {
		explorers = 1,
		writers   = 5,
		times     = 3,
		verbose   = false,
	}
	flags.parse_or_exit(&opts, os.args, .Odin)

	async.init()
	defer async.deinit()

	io.init()
	defer io.deinit()

	state := State {
		path_ch   = async.create_chan(string),
		dir_ch    = async.create_chan(string),
		file_ch   = async.create_chan(File),
		explorers = opts.explorers,
		writers   = opts.writers,
		times     = opts.times,
		verbose   = opts.verbose,
		wg        = async.create_wait_group(),
	}

	if opts.delete do state.delete_paths = make([dynamic]string, 0, 32)

	if len(opts.single) > 0 {
		async.add(state.wg)
		async.send(state.path_ch, strings.clone(opts.single))
	} else if len(opts.many) > 0 {
		buf, err := os.read_entire_file(opts.many, context.allocator)
		if err != nil {
			fmt.println("failed to read file list")
			return
		}
		defer delete(buf)

		text := transmute(string)(buf)
		for line in strings.split_lines_iterator(&text) {
			if len(line) == 0 do continue
			async.add(state.wg)
			async.send(state.path_ch, strings.clone(line))
		}
	}

	handle := async.spawn(&state, submain)
	async.block(handle, io.poll)
}

submain :: proc(state: ^State) {
	if state.verbose do fmt.println("[submain] init")
	defer if state.verbose do fmt.println("[submain] deinit")

	handles := make([dynamic]async.Handle)
	defer delete(handles)

	append(&handles, async.spawn(state, broker))
	for _ in 0 ..< state.explorers do append(&handles, async.spawn(state, explorer))
	for _ in 0 ..< state.writers do append(&handles, async.spawn(state, writer))

	async.wait(state.wg)
	async.destroy(state.path_ch)
	async.destroy(state.dir_ch)
	async.destroy(state.file_ch)
	async.destroy(state.wg)

	if state.delete_paths != nil {
		async.join(async.spawn(state, janitor))
		delete(state.delete_paths)
	}

	async.join_many(handles[:])
}

broker :: proc(state: ^State) {
	if state.verbose do fmt.println("[broker] init")
	defer if state.verbose do fmt.println("[broker] deinit")

	for path in async.recv(state.path_ch) {
		defer async.done(state.wg)

		if state.verbose do fmt.printfln("[broker] < %v", path)
		file := io.open(path, {.Write}) or_continue

		type, _, stat_err := io.stat(file)
		if stat_err != nil {
			io.close(file)
			continue
		}

		#partial switch type {
		case .Regular:
			async.add(state.wg)
			async.send(state.file_ch, File{path, file})
		case .Directory:
			io.close(file)
			async.add(state.wg)
			async.send(state.dir_ch, path)
		case:
			delete(path)
			io.close(file)
		}
	}
}

explorer :: proc(state: ^State) {
	if state.verbose do fmt.println("[explorer] init")
	defer if state.verbose do fmt.println("[explorer] deinit")

	for dir_path in async.recv(state.dir_ch) {
		defer {
			async.done(state.wg)
			queue_delete(state, dir_path)
		}

		if state.verbose do fmt.printfln("[explorer] < %v", dir_path)
		it := io.create_read_dir(dir_path) or_continue
		defer io.destroy_read_dir(&it)

		for info in io.read_dir(&it) {
			io.read_dir_error(&it) or_continue
			#partial switch info.type {
			case .Regular:
				async.add(state.wg)
				async.send(state.path_ch, strings.clone(info.fullpath))
			case .Directory:
				async.add(state.wg)
				async.send(state.dir_ch, strings.clone(info.fullpath))
			}
		}
	}
}

writer :: proc(state: ^State) {
	if state.verbose do fmt.println("[writer] init")
	defer if state.verbose do fmt.println("[writer] deinit")

	for file in async.recv(state.file_ch) {
		defer {
			async.done(state.wg)
			io.close(file.handle)
			queue_delete(state, file.path)
		}

		if state.verbose do fmt.println("[writer] <")
		_, size := io.stat(file.handle) or_continue
		for i in 0 ..< state.times do overwrite(file.handle, int(size)) or_break
	}
}

overwrite :: proc(file: io.Handle, size: int) -> bool {
	chunk := CHUNK[:]
	written := 0

	for written < size {
		remaining := size - written
		chunk_len := min(len(chunk), remaining)
		n := io.write(file, written, chunk[:chunk_len]) or_break
		written += n
	}

	return true
}

janitor :: proc(state: ^State) {
	if state.verbose do fmt.println("[janitor] init")
	defer if state.verbose do fmt.println("[janitor] deinit")

	for path in state.delete_paths {
		if state.verbose do fmt.printfln("[janitor] < %v", path)
		_ = os.remove_all(path)
		delete(path)
	}
}

@(private = "file")
queue_delete :: proc(state: ^State, path: string) {
	if state.delete_paths != nil do append(&state.delete_paths, path)
	else do delete(path)
}

