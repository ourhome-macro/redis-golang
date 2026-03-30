package main

import (
	"MiddlewareSelf/redis/client"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/peterh/liner"
)

var (
	errUnfinishedEscape = errors.New("unfinished escape")
	errUnclosedQuote    = errors.New("unclosed quote")
)

var commandKeywords = []string{
	"PING", "AUTH", "SET", "GET", "DEL", "SELECT", "SETWITHTTL",
	"HELP", "QUIT", "EXIT",
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "redis server address")
	timeout := flag.Duration("timeout", 3*time.Second, "dial/command timeout")
	flag.Parse()

	cli, err := client.DialPipeline(*addr, *timeout)
	if err != nil {
		log.Fatalf("dial %s failed: %v", *addr, err)
	}
	defer cli.Close()

	fmt.Printf("redis-cli-lite connected to %s\n", *addr)
	fmt.Println("type HELP for usage, EXIT/QUIT to quit")
	fmt.Println("tip: use ↑/↓ to browse history")
	fmt.Println("tip: press Tab for command completion, try PING")

	lineEditor := liner.NewLiner()
	defer lineEditor.Close()
	lineEditor.SetCtrlCAborts(true)
	lineEditor.SetCompleter(func(line string) (c []string) {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || !strings.Contains(trimmed, " ") {
			prefix := strings.ToUpper(trimmed)
			for _, kw := range commandKeywords {
				if strings.HasPrefix(kw, prefix) {
					c = append(c, kw)
				}
			}
		}
		return
	})

	historyPath := getHistoryPath()
	if historyPath != "" {
		if f, err := os.Open(historyPath); err == nil {
			_, _ = lineEditor.ReadHistory(f)
			_ = f.Close()
		}
	}

	for {
		line, err := readCommandLine(lineEditor, *addr+"> ")
		if err != nil {
			if err == liner.ErrPromptAborted {
				fmt.Println("")
				continue
			}
			fmt.Printf("read input failed: %v\n", err)
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		lineEditor.AppendHistory(line)

		upper := strings.ToUpper(line)
		switch upper {
		case "EXIT", "QUIT":
			saveHistory(lineEditor, historyPath)
			fmt.Println("bye")
			return
		case "HELP":
			printHelp()
			continue
		}

		args, err := splitArgs(line)
		if err != nil {
			fmt.Printf("(error) %v\n", err)
			continue
		}

		cmd := client.Command{Args: make([][]byte, 0, len(args))}
		for _, a := range args {
			cmd.Args = append(cmd.Args, []byte(a))
		}

		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		ch, err := cli.ExecStream(ctx, []client.Command{cmd})
		cancel()
		if err != nil {
			fmt.Printf("(error) %v\n", err)
			continue
		}

		for res := range ch {
			if res.Err != nil {
				fmt.Printf("(error) %v\n", res.Err)
				continue
			}
			fmt.Println(formatRESPHuman(string(res.Reply.ToBytes())))
		}
	}
}

func getHistoryPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".redis-cli-lite-history")
}

func saveHistory(editor *liner.State, path string) {
	if path == "" {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("save history failed: %v\n", err)
		return
	}
	defer f.Close()
	_, _ = editor.WriteHistory(f)
}

func printHelp() {
	fmt.Println("examples:")
	fmt.Println("  PING")
	fmt.Println("  AUTH your-password     # if server supports auth")
	fmt.Println("  SET mykey hello")
	fmt.Println("  GET mykey")
	fmt.Println("  DEL mykey")
	fmt.Println("  SELECT 1")
	fmt.Println("quote support:")
	fmt.Println("  SET greeting \"hello world\"")
	fmt.Println("multi-line support:")
	fmt.Println("  if quotes are not closed, cli will continue with ...> prompt")
}

func readCommandLine(editor *liner.State, prompt string) (string, error) {
	line, err := editor.Prompt(prompt)
	if err != nil {
		return "", err
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return "", nil
	}

	for {
		_, splitErr := splitArgs(line)
		if splitErr == nil {
			return line, nil
		}
		if !errors.Is(splitErr, errUnclosedQuote) && !errors.Is(splitErr, errUnfinishedEscape) {
			return line, nil
		}

		next, err := editor.Prompt("...> ")
		if err != nil {
			return "", err
		}
		line += "\n" + next
	}
}

// splitArgs 支持双引号参数与简单转义（\" 与 \\）。
func splitArgs(input string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inQuote := false
	escape := false

	flush := func() {
		if cur.Len() > 0 {
			args = append(args, cur.String())
			cur.Reset()
		}
	}

	for _, ch := range input {
		if escape {
			cur.WriteRune(ch)
			escape = false
			continue
		}
		if ch == '\\' {
			escape = true
			continue
		}
		if ch == '"' {
			inQuote = !inQuote
			continue
		}
		if !inQuote && (ch == ' ' || ch == '\t') {
			flush()
			continue
		}
		cur.WriteRune(ch)
	}

	if escape {
		return nil, errUnfinishedEscape
	}
	if inQuote {
		return nil, errUnclosedQuote
	}
	flush()
	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	return args, nil
}

func formatRESPHuman(raw string) string {
	if len(raw) < 3 {
		return raw
	}
	switch raw[0] {
	case '+':
		return raw[1 : len(raw)-2]
	case '-':
		return "(error) " + raw[1:len(raw)-2]
	case ':':
		return "(integer) " + raw[1:len(raw)-2]
	case '$':
		if strings.HasPrefix(raw, "$-1\r\n") {
			return "(nil)"
		}
		idx := strings.Index(raw, "\r\n")
		if idx < 0 || len(raw) < idx+4 {
			return raw
		}
		return raw[idx+2 : len(raw)-2]
	default:
		return strings.TrimSpace(raw)
	}
}
