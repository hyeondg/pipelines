package pipelines

import (
	"context"
	"errors"
	"fmt"

	"github.com/awesome-gocui/gocui"
)

// Stream statuses, shown in front of the tab names.
const (
	running = "\x1b[33m*\x1b[0m"
	passed  = "\x1b[32m✓\x1b[0m"
	failed  = "\x1b[31m✗\x1b[0m"
)

const tabBar = "\x00tabs" // a view name no stream can have

// tui is the terminal side of a Logger: a bar of tabs and, below it, one view
// per stream of which only the selected one is visible. Its state is only
// touched from the gocui main loop.
type tui struct {
	tabs     []string          // stream names in order of appearance
	marks    map[string]string // stream name -> status
	cur      int               // index of the selected tab
	finished bool              // the Logger is closed: q now quits
}

// newTUI takes over the terminal. show hands a record to the viewer, wait
// blocks until the viewer has been quit, and the returned context is cancelled
// when the user asks to stop (q, Ctrl-C) or the viewer is gone.
func newTUI(parent context.Context) (ctx context.Context, show func(record), wait func(), err error) {
	g, err := gocui.NewGui(gocui.OutputNormal, true)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	t := &tui{marks: map[string]string{}}
	g.SetManagerFunc(t.layout)

	tab := func(d int) func(*gocui.Gui) error {
		return func(*gocui.Gui) error { t.cur = (t.cur + d + len(t.tabs)) % len(t.tabs); return nil }
	}
	scroll := func(lines, pages int) func(*gocui.Gui) error {
		return func(g *gocui.Gui) error { t.scroll(g, lines, pages); return nil }
	}
	quit := func(*gocui.Gui) error {
		cancel()
		if t.finished {
			return gocui.ErrQuit
		}
		return nil
	}
	for key, fn := range map[any]func(*gocui.Gui) error{
		gocui.KeyTab: tab(1), gocui.KeyArrowRight: tab(1), gocui.KeyArrowLeft: tab(-1),
		gocui.KeyArrowUp: scroll(-1, 0), gocui.KeyArrowDown: scroll(1, 0),
		gocui.KeyPgup: scroll(0, -1), gocui.KeyPgdn: scroll(0, 1),
		gocui.KeyCtrlC: quit, 'q': quit,
	} {
		if err := g.SetKeybinding("", key, gocui.ModNone, func(g *gocui.Gui, _ *gocui.View) error { return fn(g) }); err != nil {
			g.Close()
			cancel()
			return nil, nil, nil, err
		}
	}

	exited := make(chan struct{})
	go func() {
		defer close(exited)
		g.MainLoop()
		g.Close()
		cancel()
	}()
	show = func(r record) {
		select {
		case <-exited:
		default:
			g.UpdateAsync(func(g *gocui.Gui) error { t.put(g, r); return nil })
		}
	}
	wait = func() {
		g.Update(func(*gocui.Gui) error { t.finished = true; return nil })
		select {
		case <-exited:
		case <-parent.Done(): // stopped from outside: do not wait for a key press
			g.Update(func(*gocui.Gui) error { return gocui.ErrQuit })
			<-exited
		}
	}
	return ctx, show, wait, nil
}

// view returns the view of a stream; a stream seen for the first time gets a tab.
func (t *tui) view(g *gocui.Gui, name string) *gocui.View {
	w, h := g.Size()
	v, err := g.SetView(name, 0, 1, max(w-1, 1), max(h-1, 2), 0)
	if errors.Is(err, gocui.ErrUnknownView) {
		v.Autoscroll, v.Wrap = true, true
		t.tabs = append(t.tabs, name)
	}
	return v
}

func (t *tui) put(g *gocui.Gui, r record) {
	if r.mark != "" {
		t.view(g, r.stream)
		t.marks[r.stream] = r.mark
		return
	}
	fmt.Fprintln(t.view(g, All), r.line)
	if r.stream != "" {
		fmt.Fprintln(t.view(g, r.stream), r.line)
	}
}

// layout fits the views to the terminal and redraws the tab bar.
func (t *tui) layout(g *gocui.Gui) error {
	w, _ := g.Size()
	bar, err := g.SetView(tabBar, -1, -1, w, 1, 0)
	if err != nil && !errors.Is(err, gocui.ErrUnknownView) {
		return err
	}
	bar.Frame = false
	bar.Clear()
	state, quit := "running", "cancel"
	if t.finished {
		state, quit = "finished", "quit"
	}
	t.view(g, All)
	for i, name := range t.tabs {
		v, label := t.view(g, name), " "+name+" "
		v.Title = fmt.Sprintf(" %s | Tab/Left/Right: switch | Up/Down/PgUp/PgDn: scroll | q/^C: %s ", state, quit)
		if v.Visible = i == t.cur; v.Visible {
			label = "\x1b[7m" + label + "\x1b[0m"
		}
		fmt.Fprintf(bar, " %s%s", t.marks[name], label)
	}
	return nil
}

// scroll moves the selected view; back at its end it follows the log again.
func (t *tui) scroll(g *gocui.Gui, lines, pages int) {
	v := t.view(g, t.tabs[t.cur])
	_, h := v.Size()
	_, y := v.Origin()
	end := max(len(v.ViewBufferLines())-h-1, 0)
	y = min(max(y+lines+pages*h, 0), end)
	v.Autoscroll = y == end
	v.SetOrigin(0, y)
}
