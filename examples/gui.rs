use hashseq::HashSeq;
use iced::widget::{button, checkbox, column, container, row, scrollable, text};
use iced::{Alignment, Application, Command, Element, Font, Length, Point, Rectangle, Settings, Subscription, Theme};

pub fn main() -> iced::Result {
    Demo::run(Settings {
        antialiasing: true,
        default_font: Font::MONOSPACE,
        ..Settings::default()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Example {
    ConcurrentTyping,
    CommonPrefix,
    InsertBefore,
    ForkAndMerge,
    ConcurrentRoots,
}

impl Example {
    fn name(&self) -> &'static str {
        match self {
            Example::ConcurrentTyping => "Concurrent Typing",
            Example::CommonPrefix => "Common Prefix",
            Example::InsertBefore => "InsertBefore",
            Example::ForkAndMerge => "Fork & Merge",
            Example::ConcurrentRoots => "Concurrent Roots",
        }
    }

    fn description(&self) -> &'static str {
        match self {
            Example::ConcurrentTyping => "Two sentences, no interleaving",
            Example::CommonPrefix => "Same start, different endings",
            Example::InsertBefore => "Code edits with InsertBefore",
            Example::ForkAndMerge => "Shared todo list, both add items",
            Example::ConcurrentRoots => "Two stories start simultaneously",
        }
    }

    fn setup(&self) -> (HashSeq, HashSeq) {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // Helper to type a string at a position
        let type_at = |seq: &mut HashSeq, pos: usize, text: &str| {
            for (i, c) in text.chars().enumerate() {
                seq.insert(pos + i, c);
            }
        };

        match self {
            Example::ConcurrentTyping => {
                // User A types a sentence
                type_at(&mut seq_a, 0, "The quick brown fox");
                // User B types a different sentence concurrently
                type_at(&mut seq_b, 0, "jumps over the lazy dog");
            }
            Example::CommonPrefix => {
                // Both users start typing the same thing, then diverge
                type_at(&mut seq_a, 0, "Dear Alice, I hope this message finds you well.");
                type_at(&mut seq_b, 0, "Dear Alice, I wanted to tell you something.");
            }
            Example::InsertBefore => {
                // User A creates a document
                type_at(&mut seq_a, 0, "function() { return x; }");
                // Sync so B has the same
                seq_b.merge(seq_a.clone());
                // User A adds parameter
                type_at(&mut seq_a, 9, "name");
                // User B inserts code inside the function body (between { and return)
                type_at(&mut seq_b, 13, "let y = x * 2; ");
            }
            Example::ForkAndMerge => {
                // Shared base: a todo list
                type_at(&mut seq_a, 0, "TODO:\n- Buy groceries\n");
                seq_b.merge(seq_a.clone());

                // User A adds items
                let pos_a = seq_a.len();
                type_at(&mut seq_a, pos_a, "- Call mom\n- Fix bike\n");

                // User B adds different items
                let pos_b = seq_b.len();
                type_at(&mut seq_b, pos_b, "- Send email\n- Read book\n");
            }
            Example::ConcurrentRoots => {
                // Both users start typing from empty doc simultaneously
                type_at(&mut seq_a, 0, "Chapter 1: The Beginning");
                type_at(&mut seq_b, 0, "Once upon a time...");
            }
        }

        (seq_a, seq_b)
    }

    const ALL: [Example; 5] = [
        Example::ConcurrentTyping,
        Example::CommonPrefix,
        Example::InsertBefore,
        Example::ForkAndMerge,
        Example::ConcurrentRoots,
    ];
}

#[derive(Default)]
struct Demo {
    seq_seq: usize, // sequence number of which seq we are on.
    seq_a: HashSeq,
    seq_a_viz: hashseq_viz::State,
    seq_b: HashSeq,
    seq_b_viz: hashseq_viz::State,
    show_dependencies: bool,
    selected_example: Option<Example>,
    redraw_counter: usize,
}

#[derive(Debug, Clone, Copy)]
enum Message {
    Clear,
    SeqA(hashseq_viz::Msg),
    SeqB(hashseq_viz::Msg),
    MergeAtoB,
    MergeBtoA,
    Sync,
    ShowDependencies(bool),
    LoadExample(Example),
    Tick,
}

impl Application for Demo {
    type Executor = iced::executor::Default;
    type Message = Message;
    type Theme = Theme;
    type Flags = ();

    fn new(_flags: ()) -> (Self, Command<Message>) {
        (Demo::default(), Command::none())
    }

    fn title(&self) -> String {
        String::from("HashSeq Demo")
    }

    fn update(&mut self, message: Message) -> Command<Message> {
        match dbg!(message) {
            Message::Clear => {
                self.seq_a_viz = hashseq_viz::State::default();
                self.seq_a = HashSeq::default();
                self.seq_b_viz = hashseq_viz::State::default();
                self.seq_b = HashSeq::default();
                self.seq_seq += 1;
            }
            Message::SeqA(hashseq_viz::Msg::Insert(idx, c)) => {
                self.seq_a.insert(idx, c);
            }
            Message::SeqA(hashseq_viz::Msg::Remove(idx)) => {
                self.seq_a.remove(idx);
            }
            Message::SeqB(hashseq_viz::Msg::Insert(idx, c)) => {
                self.seq_b.insert(idx, c);
            }
            Message::SeqB(hashseq_viz::Msg::Remove(idx)) => {
                self.seq_b.remove(idx);
            }
            Message::SeqA(hashseq_viz::Msg::Tick) | Message::SeqB(hashseq_viz::Msg::Tick) => {
                // Handled by global tick
            }
            Message::MergeAtoB => {
                self.seq_b.merge(self.seq_a.clone());
            }
            Message::MergeBtoA => {
                self.seq_a.merge(self.seq_b.clone());
            }
            Message::Sync => {
                let seq_a = self.seq_a.clone();
                self.seq_a.merge(self.seq_b.clone());
                self.seq_b.merge(seq_a);
            }
            Message::ShowDependencies(v) => {
                self.show_dependencies = v;
            }
            Message::LoadExample(example) => {
                let (seq_a, seq_b) = example.setup();
                self.seq_a = seq_a;
                self.seq_b = seq_b;
                self.seq_a_viz = hashseq_viz::State::default();
                self.seq_b_viz = hashseq_viz::State::default();
                self.seq_seq += 1;
                self.selected_example = Some(example);
            }
            Message::Tick => {
                self.redraw_counter = self.redraw_counter.wrapping_add(1);
                // Run physics on each tick
                self.seq_a_viz.tick(&self.seq_a);
                self.seq_b_viz.tick(&self.seq_b);
            }
        }
        Command::none()
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(std::time::Duration::from_millis(16)).map(|_| Message::Tick)
    }

    fn view(&self) -> Element<'_, Message> {
        // Sidebar with examples
        let mut example_buttons = column![text("Examples").size(20)]
            .spacing(8)
            .padding(10)
            .width(Length::Fixed(180.0));

        for example in Example::ALL {
            let is_selected = self.selected_example == Some(example);
            let btn = button(
                column![
                    text(example.name()).size(14),
                    text(example.description()).size(10),
                ]
                .spacing(2),
            )
            .padding(8)
            .width(Length::Fill)
            .on_press(Message::LoadExample(example));

            let btn = if is_selected {
                btn.style(iced::theme::Button::Primary)
            } else {
                btn.style(iced::theme::Button::Secondary)
            };

            example_buttons = example_buttons.push(btn);
        }

        let sidebar = container(scrollable(example_buttons))
            .height(Length::Fill)
            .style(iced::theme::Container::Box);

        // Main content
        let main_content = column![
            // Hidden frame counter to force iced to redraw (size 1 makes it invisible)
            text(format!("{}", self.redraw_counter)).size(1),
            self.seq_a_viz
                .view(self.seq_seq, self.redraw_counter, &self.seq_a, self.show_dependencies)
                .map(Message::SeqA),
            row![
                button("merge down").padding(8).on_press(Message::MergeAtoB),
                button("sync").padding(8).on_press(Message::Sync),
                button("merge up").padding(8).on_press(Message::MergeBtoA)
            ]
            .spacing(20),
            self.seq_b_viz
                .view(self.seq_seq, self.redraw_counter, &self.seq_b, self.show_dependencies)
                .map(Message::SeqB),
            row![
                button("Clear").padding(8).on_press(Message::Clear),
                checkbox(
                    "Show dependencies",
                    self.show_dependencies,
                    Message::ShowDependencies
                ),
            ]
            .spacing(20),
        ]
        .padding(20)
        .spacing(20)
        .align_items(Alignment::Center)
        .width(Length::Fill);

        row![sidebar, main_content]
            .height(Length::Fill)
            .into()
    }
}

mod hashseq_viz {
    use std::collections::BTreeMap;

    use super::*;
    use hashseq::Id;
    use iced::keyboard;
    use iced::widget::canvas::event::{self, Event};
    use iced::widget::canvas::{self, Canvas, Fill, Frame, Geometry, Path, Stroke, Text};
    use iced::{Color, Font, Renderer, Size, Vector, mouse};

    #[derive(Debug, Clone, Copy)]
    pub enum Msg {
        Insert(usize, char),
        Remove(usize),
        Tick,
    }

    #[derive(Default)]
    pub struct State {
        node_pos: BTreeMap<Id, Point>,
    }

    impl State {
        pub fn view<'a>(
            &'a self,
            seq_seq: usize,
            redraw_counter: usize,
            seq: &'a HashSeq,
            show_dependencies: bool,
        ) -> Element<'a, Msg> {
            // Height variation forces iced to re-layout and redraw the canvas
            Canvas::new(HashSeqDemo {
                state: self,
                seq_seq,
                seq,
                show_dependencies,
            })
            .width(Length::Fill)
            .height(Length::FillPortion(1000 + (redraw_counter % 2) as u16))
            .into()
        }

        pub fn tick(&mut self, seq: &HashSeq) {
            // Use default bounds for physics - nodes will be recentered anyway
            let bounds = Rectangle::new(Point::ORIGIN, Size::new(800.0, 300.0));
            self.run_physics(seq, bounds);
        }

        fn run_physics(&mut self, seq: &HashSeq, bounds: Rectangle) {
            let k = 0.2;
            let h_spacing = 30.0;
            let v_spacing = 28.0;
            let text_size = 14.0;
            let char_width = text_size * 0.6;
            let padding = 4.0;

            // Helper to get position of any node, including characters inside runs
            let get_node_pos = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                if let Some(pos) = nodes.get(id) {
                    return Some(*pos);
                }
                // Check if this ID is inside a run
                if let Some(run_pos) = seq.run_index.get(id) {
                    // Get the run's first ID to find its position
                    if let Some(run) = seq.runs.get(&run_pos.run_id) {
                        return nodes.get(&run.first_id()).copied();
                    }
                }
                None
            };

            // Helper to get the right edge of a node (for InsertAfter positioning)
            let get_node_right_edge = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                // Check if id IS a run
                if let Some(run) = seq.runs.get(id)
                    && let Some(center) = nodes.get(id)
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x + width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is INSIDE a run
                if let Some(run_pos) = seq.run_index.get(id)
                    && let Some(run) = seq.runs.get(&run_pos.run_id)
                    && let Some(center) = nodes.get(&run.first_id())
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x + width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is a root node
                if seq.root_nodes.contains_key(id)
                    && let Some(center) = nodes.get(id)
                {
                    let width = char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x + width / 2.0,
                        y: center.y,
                    });
                }
                // For other individual nodes, use center position
                get_node_pos(id, nodes)
            };

            // Helper to get the left edge of a node (for InsertBefore positioning)
            let get_node_left_edge = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                // Check if id IS a run
                if let Some(run) = seq.runs.get(id)
                    && let Some(center) = nodes.get(id)
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x - width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is INSIDE a run
                if let Some(run_pos) = seq.run_index.get(id)
                    && let Some(run) = seq.runs.get(&run_pos.run_id)
                    && let Some(center) = nodes.get(&run.first_id())
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x - width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is a root node
                if seq.root_nodes.contains_key(id)
                    && let Some(center) = nodes.get(id)
                {
                    let width = char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x - width / 2.0,
                        y: center.y,
                    });
                }
                // For other individual nodes, use center position
                get_node_pos(id, nodes)
            };

            let pos_in_set =
                |id: Id, set: Vec<&Id>, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                    // Find position of id in the set, then get neighbors
                    let mut sorted_set: Vec<Id> = set.iter().map(|&i| *i).collect();
                    sorted_set.sort();
                    let pos = sorted_set.iter().position(|i| *i == id);
                    let before_id = pos.and_then(|p| if p > 0 { sorted_set.get(p - 1).cloned() } else { None });
                    let after_id = pos.and_then(|p| sorted_set.get(p + 1).cloned());
                    let before_pos = before_id.and_then(|id| get_node_pos(&id, nodes));
                    let after_pos = after_id.and_then(|id| get_node_pos(&id, nodes));

                    match (before_pos, after_pos) {
                        (None, None) => None,
                        (None, Some(after)) => Some(Point {
                            x: after.x,
                            y: after.y - v_spacing,
                        }),
                        (Some(before), None) => Some(Point {
                            x: before.x,
                            y: before.y + v_spacing,
                        }),
                        (Some(before), Some(after)) => Some(Point {
                            x: (before.x + after.x) / 2.0,
                            y: (before.y + after.y) / 2.0,
                        }),
                    }
                };

            let mut i = 0;
            loop {
                i += 1;
                let mut net_change = 0.0;

                // Process root nodes - stratify concurrent roots into lanes
                let roots: Vec<Id> = seq.root_nodes.keys().copied().collect();
                let num_roots = roots.len();
                let mut sorted_roots = roots.clone();
                sorted_roots.sort();

                for id in seq.root_nodes.keys() {
                    let pos = *self.node_pos.entry(*id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });

                    // Calculate lane offset for concurrent roots
                    let root_idx = sorted_roots.iter().position(|r| r == id).unwrap_or(0);
                    let lane_offset = if num_roots > 1 {
                        let lane_height = v_spacing * 2.5;
                        let total_height = lane_height * (num_roots - 1) as f32;
                        let base_offset = -total_height / 2.0;
                        base_offset + lane_height * root_idx as f32
                    } else {
                        0.0
                    };

                    let roots_vec: Vec<&Id> = seq.root_nodes.keys().collect();
                    let target_pos = match pos_in_set(*id, roots_vec, &self.node_pos) {
                        Some(p) => Point { x: p.x, y: p.y + lane_offset },
                        None => Point { x: pos.x, y: bounds.height / 2.0 + lane_offset },
                    };

                    let delta = Vector::<f32> {
                        x: target_pos.x - pos.x,
                        y: target_pos.y - pos.y,
                    };

                    let push = delta * k;
                    net_change += (push.x.powf(2.0) + push.y.powf(2.0)).sqrt();
                    let pos = self.node_pos.entry(*id).or_default();
                    pos.x += push.x;
                    pos.y += push.y;
                }

                // Process before nodes - stratify concurrent before nodes into lanes
                for (id, before_node) in seq.before_nodes.iter() {
                    let pos = *self.node_pos.entry(*id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });
                    let parent = &before_node.anchor;
                    let target_pos = if let Some(p) = get_node_left_edge(parent, &self.node_pos) {
                        // Get all siblings (nodes before the same parent)
                        let siblings = seq.befores(parent);

                        // Find this node's index among siblings
                        let mut sorted_siblings: Vec<Id> = siblings.into_iter().copied().collect();
                        sorted_siblings.sort();
                        let sibling_idx = sorted_siblings.iter().position(|s| s == id).unwrap_or(0);

                        // Calculate lane offset - always offset below the anchor
                        let lane_height = v_spacing * 2.5;
                        let base_offset = lane_height; // Start one lane below the anchor
                        let lane_offset = base_offset + lane_height * sibling_idx as f32;

                        Point {
                            x: p.x - h_spacing,
                            y: p.y + lane_offset,
                        }
                    } else {
                        pos
                    };

                    let delta = Vector::<f32> {
                        x: target_pos.x - pos.x,
                        y: target_pos.y - pos.y,
                    };

                    let push = delta * k;
                    net_change += (push.x.powf(2.0) + push.y.powf(2.0)).sqrt();
                    let pos = self.node_pos.entry(*id).or_default();
                    pos.x += push.x;
                    pos.y += push.y;
                }

                // Process remove nodes
                for (id, remove_node) in seq.remove_nodes.iter() {
                    let pos = *self.node_pos.entry(*id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });
                    let targets = &remove_node.nodes;
                    let target_pos = if !targets.is_empty() {
                        let p: Vector = targets
                            .iter()
                            .filter_map(|t| get_node_pos(t, &self.node_pos))
                            .map(|p| Vector::new(p.x, p.y))
                            .reduce(|accum, p| accum + p)
                            .unwrap_or_default();
                        Point {
                            x: p.x,
                            y: p.y - 5.0,
                        }
                    } else {
                        pos
                    };

                    let delta = Vector::<f32> {
                        x: target_pos.x - pos.x,
                        y: target_pos.y - pos.y,
                    };

                    let push = delta * k;
                    net_change += (push.x.powf(2.0) + push.y.powf(2.0)).sqrt();
                    let pos = self.node_pos.entry(*id).or_default();
                    pos.x += push.x;
                    pos.y += push.y;
                }

                // Process runs - position each run as a single entity
                for (run_id, run) in seq.runs.iter() {
                    let pos = *self.node_pos.entry(*run_id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });

                    let left_pos = get_node_left_edge(run_id, &self.node_pos).unwrap_or(pos);

                    // Determine target position based on run structure
                    let target_pos = {
                        // Has left dependencies
                        let parent = run.insert_after;
                        if let Some(p) = get_node_right_edge(&parent, &self.node_pos) {
                            // Check how many siblings this run has (concurrent branches from same parent)
                            let siblings = seq.afters(&parent);
                            let num_siblings = siblings.len();

                            // Find this run's index among siblings (sorted by Id for consistency)
                            let mut sorted_siblings: Vec<Id> = siblings.iter().map(|&id| *id).collect();
                            sorted_siblings.sort();
                            let sibling_idx = sorted_siblings.iter().position(|id| id == run_id).unwrap_or(0);

                            // Calculate vertical offset to spread siblings into lanes
                            let lane_offset = if num_siblings > 1 {
                                let lane_height = v_spacing * 2.5;
                                let total_height = lane_height * (num_siblings - 1) as f32;
                                let base_offset = -total_height / 2.0;
                                base_offset + lane_height * sibling_idx as f32
                            } else {
                                0.0
                            };

                            Point {
                                x: p.x + h_spacing,
                                y: p.y + lane_offset,
                            }
                        } else {
                            left_pos
                        }
                    };

                    let delta = Vector::<f32> {
                        x: target_pos.x - left_pos.x,
                        y: target_pos.y - left_pos.y,
                    };

                    let push = delta * k;
                    net_change += (push.x.powf(2.0) + push.y.powf(2.0)).sqrt();
                    let pos = self.node_pos.entry(*run_id).or_default();
                    pos.x += push.x;
                    pos.y += push.y;
                }

                if i > 10 || net_change < 1e-4 {
                    break;
                }
            }

            if !self.node_pos.is_empty() {
                // Recenter things
                let mut avg_pos = Point::ORIGIN;
                for (_, pos) in self.node_pos.iter() {
                    avg_pos.x += pos.x;
                    avg_pos.y += pos.y;
                }
                avg_pos.x /= self.node_pos.len() as f32;
                avg_pos.y /= self.node_pos.len() as f32;
                let dx = bounds.width / 2.0 - avg_pos.x;
                let dy = bounds.height / 2.0 - avg_pos.y;

                for (_, pos) in self.node_pos.iter_mut() {
                    pos.x += dx;
                    pos.y += dy;
                }
            }
        }
    }

    struct HashSeqDemo<'a> {
        state: &'a State,
        seq_seq: usize,
        seq: &'a HashSeq,
        show_dependencies: bool,
    }

    #[derive(Default)]
    struct ProgramState {
        seq_seq: usize,
        cursor: usize,
    }

    impl<'a> canvas::Program<Msg> for HashSeqDemo<'a> {
        type State = ProgramState;

        fn update(
            &self,
            state: &mut Self::State,
            event: Event,
            _bounds: Rectangle,
            cursor: mouse::Cursor,
        ) -> (event::Status, Option<Msg>) {
            let cursor_in_bounds = cursor.is_over(_bounds);

            // Reset cursor when example changes
            if self.seq_seq != state.seq_seq {
                state.seq_seq = self.seq_seq;
                state.cursor = 0;
            }

            // Clamp cursor to valid range (document may have changed after merge/sync)
            state.cursor = state.cursor.min(self.seq.len());

            if cursor_in_bounds {
                match event {
                    Event::Keyboard(kbd_event) => {
                        let msg = match kbd_event {
                            keyboard::Event::KeyPressed {
                                key_code: keyboard::KeyCode::Backspace,
                                ..
                            } => {
                                state.cursor = state.cursor.saturating_sub(1);
                                Some(Msg::Remove(state.cursor))
                            }
                            keyboard::Event::KeyPressed {
                                key_code: keyboard::KeyCode::Left,
                                ..
                            } => {
                                state.cursor = state.cursor.saturating_sub(1);
                                Some(Msg::Tick)
                            }
                            keyboard::Event::KeyPressed {
                                key_code: keyboard::KeyCode::Right,
                                ..
                            } => {
                                state.cursor = (state.cursor + 1).min(self.seq.len());
                                Some(Msg::Tick)
                            }
                            keyboard::Event::CharacterReceived(c) if !c.is_control() => {
                                let insert_idx = state.cursor;
                                state.cursor += 1;
                                Some(Msg::Insert(insert_idx, c))
                            }
                            _ => None,
                        };
                        (event::Status::Captured, msg)
                    }
                    _ => (event::Status::Ignored, None),
                }
            } else {
                (event::Status::Ignored, None)
            }
        }

        fn draw(
            &self,
            state: &Self::State,
            renderer: &Renderer,
            _theme: &Theme,
            bounds: Rectangle,
            _cursor: mouse::Cursor,
        ) -> Vec<Geometry> {
            let mut stack = Vec::new();
            // Always render - don't skip if seq_seq doesn't match
            // (state.seq_seq is only updated in update(), which may not have run yet)
            {
                let mut frame = Frame::new(renderer, bounds.size());
                {
                            let text_size = 14.0;
                            let char_width = text_size * 0.6;
                            let padding = 4.0;

                            // Helper to get center position of any node
                            let get_node_pos = |id: &Id| -> Option<Point> {
                                if let Some(pos) = self.state.node_pos.get(id) {
                                    return Some(*pos);
                                }
                                // Check if this ID is inside a run
                                if let Some(run_pos) = self.seq.run_index.get(id) {
                                    return self.state.node_pos.get(&run_pos.run_id).copied();
                                }
                                None
                            };

                            // Helper to get the width of a node's bounding box (includes removed chars)
                            let get_node_width = |id: &Id| -> f32 {
                                if let Some(run) = self.seq.runs.get(id) {
                                    run.run.chars().count() as f32 * char_width
                                } else if let Some(run_pos) = self.seq.run_index.get(id) {
                                    // ID is inside a run - get the run's width
                                    if let Some(run) = self.seq.runs.get(&run_pos.run_id) {
                                        run.run.chars().count() as f32 * char_width
                                    } else {
                                        0.0
                                    }
                                } else if self.seq.root_nodes.contains_key(id)
                                    || self.seq.before_nodes.contains_key(id)
                                {
                                    char_width + padding * 2.0
                                } else {
                                    0.0 // Point node (no width)
                                }
                            };

                            // Helper to get left edge center of a node (for incoming edges)
                            let get_node_left_edge = |id: &Id| -> Option<Point> {
                                let center = get_node_pos(id)?;
                                let width = get_node_width(id);
                                if width > 0.0 {
                                    Some(Point { x: center.x - width / 2.0, y: center.y })
                                } else {
                                    Some(center)
                                }
                            };

                            // Helper to get right edge center of a node (for outgoing edges)
                            let get_node_right_edge = |id: &Id| -> Option<Point> {
                                let center = get_node_pos(id)?;
                                let width = get_node_width(id);
                                if width > 0.0 {
                                    Some(Point { x: center.x + width / 2.0, y: center.y })
                                } else {
                                    Some(center)
                                }
                            };

                            let string = String::from_iter(self.seq.iter());
                            let cursor_pos = state.cursor.min(string.chars().count());
                            let before_cursor =
                                String::from_iter(string.chars().take(cursor_pos));
                            let after_cursor = String::from_iter(string.chars().skip(cursor_pos));
                            let mut text = Text::from(format!("{before_cursor}|{after_cursor}"));
                            text.size = 20.0;
                            text.font = Font::MONOSPACE;
                            frame.fill_text(text);

                            // Draw "after" edges (green) - from right edge to left edge
                            for (id, afters) in self.seq.afters.iter() {
                                let Some(from) = get_node_right_edge(id) else {
                                    continue;
                                };
                                for after in afters.iter() {
                                    let Some(to) = get_node_left_edge(after) else {
                                        continue;
                                    };
                                    frame.stroke(
                                        &Path::line(from, to),
                                        Stroke::default()
                                            .with_color(Color::from_rgb(0.0, 1.0, 0.0)),
                                    );
                                }
                            }
                            // Draw "before" edges (red) - from left edge to center of before node
                            for (id, befores) in self.seq.befores_by_anchor.iter() {
                                let Some(from) = get_node_left_edge(id) else {
                                    continue;
                                };
                                for before in befores {
                                    let Some(to) = get_node_pos(before) else {
                                        continue;
                                    };
                                    frame.stroke(
                                        &Path::line(from, to),
                                        Stroke::default()
                                            .with_color(Color::from_rgb(1.0, 0.0, 0.0)),
                                    );
                                }
                            }

                            // Render all nodes (both individual and runs)
                            for (id, pos) in self.state.node_pos.iter() {
                                // Check if this ID corresponds to a run
                                if let Some(run) = self.seq.runs.get(id) {
                                    // Decompress to get individual character nodes
                                    let nodes = run.decompress();
                                    let num_chars = nodes.len();

                                    let total_width = num_chars as f32 * char_width;
                                    let height = text_size + padding * 2.0;
                                    let start_x = pos.x - total_width / 2.0;

                                    // Draw individual character boxes
                                    for (i, node) in nodes.iter().enumerate() {
                                        let is_removed = self.seq.removed_inserts.contains(&node.id());
                                        let char_x = start_x + i as f32 * char_width;

                                        // Draw character background
                                        let bg_color = if is_removed {
                                            Color::from_rgba(0.5, 0.5, 0.5, 0.7) // Gray for removed
                                        } else {
                                            Color::from_rgb(0.0, 0.5, 1.0) // Normal blue
                                        };

                                        frame.fill(
                                            &Path::rectangle(
                                                Point {
                                                    x: char_x,
                                                    y: pos.y - height / 2.0,
                                                },
                                                Size::new(char_width, height),
                                            ),
                                            Fill::from(bg_color),
                                        );

                                        // Draw character
                                        let ch = match &node.op {
                                            hashseq::Op::InsertAfter(_, c) => *c,
                                            _ => '?',
                                        };
                                        let mut text = Text::from(ch.to_string());
                                        text.position = Point {
                                            x: char_x,
                                            y: pos.y - text_size / 2.0 + 2.0,
                                        };
                                        text.size = text_size;
                                        text.font = Font::MONOSPACE;
                                        text.color = if is_removed {
                                            Color::from_rgba(1.0, 1.0, 1.0, 0.5)
                                        } else {
                                            Color::WHITE
                                        };
                                        frame.fill_text(text);

                                        // Draw strikethrough for removed characters
                                        if is_removed {
                                            frame.stroke(
                                                &Path::line(
                                                    Point { x: char_x, y: pos.y },
                                                    Point { x: char_x + char_width, y: pos.y },
                                                ),
                                                Stroke::default()
                                                    .with_width(2.0)
                                                    .with_color(Color::from_rgba(1.0, 0.0, 0.0, 0.8)),
                                            );
                                        }
                                    }
                                } else if let Some(root) = self.seq.root_nodes.get(id) {
                                    // Render root node as a box (like runs) with different color
                                    let is_removed = self.seq.removed_inserts.contains(id);
                                    let ch_str = format!("{}", root.ch);
                                    let width = ch_str.chars().count() as f32 * char_width + padding * 2.0;
                                    let height = text_size + padding * 2.0;

                                    // Draw rectangle background (green for roots, gray if removed)
                                    let rect_pos = Point {
                                        x: pos.x - width / 2.0,
                                        y: pos.y - height / 2.0,
                                    };
                                    let bg_color = if is_removed {
                                        Color::from_rgba(0.5, 0.5, 0.5, 0.5)
                                    } else {
                                        Color::from_rgb(0.2, 0.7, 0.3)
                                    };
                                    frame.fill(
                                        &Path::rectangle(rect_pos, Size::new(width, height)),
                                        Fill::from(bg_color),
                                    );

                                    // Draw text centered
                                    let mut text = Text::from(ch_str);
                                    text.position = Point {
                                        x: pos.x - char_width / 2.0,
                                        y: pos.y - text_size / 2.0 + 2.0,
                                    };
                                    text.size = text_size;
                                    text.font = Font::MONOSPACE;
                                    text.color = if is_removed {
                                        Color::from_rgba(1.0, 1.0, 1.0, 0.5)
                                    } else {
                                        Color::WHITE
                                    };
                                    frame.fill_text(text);

                                    // Draw strikethrough if removed
                                    if is_removed {
                                        frame.stroke(
                                            &Path::line(
                                                Point { x: rect_pos.x, y: pos.y },
                                                Point { x: rect_pos.x + width, y: pos.y },
                                            ),
                                            Stroke::default()
                                                .with_width(2.0)
                                                .with_color(Color::from_rgba(1.0, 0.0, 0.0, 0.7)),
                                        );
                                    }

                                    // Render dependencies for root nodes
                                    if self.show_dependencies {
                                        for dep in root.extra_dependencies.iter() {
                                            if let Some(dep_from) = get_node_pos(dep) {
                                                let mid = Point {
                                                    x: (pos.x + dep_from.x) / 2.0,
                                                    y: (pos.y + dep_from.y) / 2.0 - 20.0,
                                                };
                                                let curve = Path::new(|p| {
                                                    p.move_to(dep_from);
                                                    p.quadratic_curve_to(mid, *pos);
                                                });

                                                frame.stroke(
                                                    &curve,
                                                    Stroke::default().with_width(1.0).with_color(
                                                        Color::from_rgba(0.0, 0.0, 0.0, 0.5),
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                } else if let Some(before) = self.seq.before_nodes.get(id) {
                                    // Render before node as a box with different color
                                    let is_removed = self.seq.removed_inserts.contains(id);
                                    let ch_str = format!("{}", before.ch);
                                    let width = ch_str.chars().count() as f32 * char_width + padding * 2.0;
                                    let height = text_size + padding * 2.0;

                                    // Draw rectangle background (orange for before nodes, gray if removed)
                                    let rect_pos = Point {
                                        x: pos.x - width / 2.0,
                                        y: pos.y - height / 2.0,
                                    };
                                    let bg_color = if is_removed {
                                        Color::from_rgba(0.5, 0.5, 0.5, 0.5)
                                    } else {
                                        Color::from_rgb(0.9, 0.6, 0.2)
                                    };
                                    frame.fill(
                                        &Path::rectangle(rect_pos, Size::new(width, height)),
                                        Fill::from(bg_color),
                                    );

                                    // Draw text centered
                                    let mut text = Text::from(ch_str);
                                    text.position = Point {
                                        x: pos.x - char_width / 2.0,
                                        y: pos.y - text_size / 2.0 + 2.0,
                                    };
                                    text.size = text_size;
                                    text.font = Font::MONOSPACE;
                                    text.color = if is_removed {
                                        Color::from_rgba(1.0, 1.0, 1.0, 0.5)
                                    } else {
                                        Color::WHITE
                                    };
                                    frame.fill_text(text);

                                    // Draw strikethrough if removed
                                    if is_removed {
                                        frame.stroke(
                                            &Path::line(
                                                Point { x: rect_pos.x, y: pos.y },
                                                Point { x: rect_pos.x + width, y: pos.y },
                                            ),
                                            Stroke::default()
                                                .with_width(2.0)
                                                .with_color(Color::from_rgba(1.0, 0.0, 0.0, 0.7)),
                                        );
                                    }

                                    // Render dependencies for before nodes
                                    if self.show_dependencies {
                                        for dep in before.extra_dependencies.iter() {
                                            if let Some(dep_from) = get_node_pos(dep) {
                                                let mid = Point {
                                                    x: (pos.x + dep_from.x) / 2.0,
                                                    y: (pos.y + dep_from.y) / 2.0 - 20.0,
                                                };
                                                let curve = Path::new(|p| {
                                                    p.move_to(dep_from);
                                                    p.quadratic_curve_to(mid, *pos);
                                                });

                                                frame.stroke(
                                                    &curve,
                                                    Stroke::default().with_width(1.0).with_color(
                                                        Color::from_rgba(0.0, 0.0, 0.0, 0.5),
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                } else if self.seq.remove_nodes.contains_key(id) {
                                    // Skip rendering remove nodes - removals are shown via strikethrough on affected chars
                                }
                            }
                }
                stack.push(frame.into_geometry());
            }
            stack
        }

        fn mouse_interaction(
            &self,
            _state: &Self::State,
            bounds: Rectangle,
            cursor: mouse::Cursor,
        ) -> mouse::Interaction {
            if cursor.is_over(bounds) {
                mouse::Interaction::Crosshair
            } else {
                mouse::Interaction::default()
            }
        }
    }
}
