use hashseq::HashSeq;
use iced::widget::{button, checkbox, column, row, text};
use iced::{Alignment, Element, Length, Point, Rectangle, Sandbox, Settings, Theme};

pub fn main() -> iced::Result {
    Demo::run(Settings {
        antialiasing: true,
        ..Settings::default()
    })
}

#[derive(Default)]
struct Demo {
    seq_seq: usize, // sequence number of which seq we are on.
    seq_a: HashSeq,
    seq_a_viz: hashseq_viz::State,
    seq_b: HashSeq,
    seq_b_viz: hashseq_viz::State,
    show_dependencies: bool,
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
}

impl Sandbox for Demo {
    type Message = Message;

    fn new() -> Self {
        Demo::default()
    }

    fn title(&self) -> String {
        String::from("HashSeq Demo")
    }

    fn update(&mut self, message: Message) {
        match dbg!(message) {
            Message::Clear => {
                self.seq_a_viz = hashseq_viz::State::default();
                self.seq_a = HashSeq::default();
                self.seq_b_viz = hashseq_viz::State::default();
                self.seq_b = HashSeq::default();
                self.seq_seq += 1;
                self.seq_a_viz.request_redraw();
                self.seq_b_viz.request_redraw();
            }
            Message::SeqA(hashseq_viz::Msg::Insert(idx, c)) => {
                self.seq_a.insert(idx, c);
                self.seq_a_viz.request_redraw();
            }
            Message::SeqA(hashseq_viz::Msg::Remove(idx)) => {
                self.seq_a.remove(idx);
                self.seq_a_viz.request_redraw();
            }
            Message::SeqB(hashseq_viz::Msg::Insert(idx, c)) => {
                self.seq_b.insert(idx, c);
                self.seq_b_viz.request_redraw();
            }
            Message::SeqB(hashseq_viz::Msg::Remove(idx)) => {
                self.seq_b.remove(idx);
                self.seq_b_viz.request_redraw();
            }
            Message::SeqA(hashseq_viz::Msg::Tick) => {
                self.seq_a_viz.request_redraw();
            }
            Message::SeqB(hashseq_viz::Msg::Tick) => {
                self.seq_b_viz.request_redraw();
            }
            Message::MergeAtoB => {
                self.seq_b.merge(self.seq_a.clone());
                self.seq_b_viz.request_redraw();
            }
            Message::MergeBtoA => {
                self.seq_a.merge(self.seq_b.clone());
                self.seq_a_viz.request_redraw();
            }
            Message::Sync => {
                let seq_a = self.seq_a.clone();
                self.seq_a.merge(self.seq_b.clone());
                self.seq_b.merge(seq_a);
                self.seq_a_viz.request_redraw();
                self.seq_b_viz.request_redraw();
            }
            Message::ShowDependencies(v) => {
                self.show_dependencies = v;
            }
        }
    }

    fn view(&self) -> Element<'_, Message> {
        column![
            text("HashSeq Demo").width(Length::Shrink).size(36),
            self.seq_a_viz
                .view(self.seq_seq, &self.seq_a, self.show_dependencies)
                .map(Message::SeqA),
            row![
                button("merge down").padding(8).on_press(Message::MergeAtoB),
                button("sync").padding(8).on_press(Message::Sync),
                button("merge up").padding(8).on_press(Message::MergeBtoA)
            ]
            .spacing(20),
            self.seq_b_viz
                .view(self.seq_seq, &self.seq_b, self.show_dependencies)
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
        .into()
    }
}

mod hashseq_viz {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use hashseq::Id;
    use iced::keyboard::KeyCode;
    use iced::widget::canvas::event::{self, Event};
    use iced::widget::canvas::{self, Canvas, Fill, Frame, Geometry, Path, Stroke, Text};
    use iced::{Color, Renderer, Size, Vector, mouse};

    #[derive(Debug, Clone, Copy)]
    pub enum Msg {
        Insert(usize, char),
        Remove(usize),
        Tick,
    }

    #[derive(Default)]
    pub struct State {
        cache: canvas::Cache,
    }

    impl State {
        pub fn view<'a>(
            &'a self,
            seq_seq: usize,
            seq: &'a HashSeq,
            show_dependencies: bool,
        ) -> Element<'a, Msg> {
            Canvas::new(HashSeqDemo {
                seq_seq,
                state: self,
                seq,
                show_dependencies,
            })
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
        }

        pub fn request_redraw(&mut self) {
            self.cache.clear()
        }
    }

    struct HashSeqDemo<'a> {
        seq_seq: usize,
        seq: &'a HashSeq,
        state: &'a State,
        show_dependencies: bool,
    }

    #[derive(Default)]
    struct ProgramState {
        seq_seq: usize,
        cursor: usize,
        node_pos: BTreeMap<Id, Point>,
    }

    impl<'a> canvas::Program<Msg> for HashSeqDemo<'a> {
        type State = ProgramState;

        fn update(
            &self,
            state: &mut Self::State,
            event: Event,
            bounds: Rectangle,
            cursor: mouse::Cursor,
        ) -> (event::Status, Option<Msg>) {
            if cursor.position_in(bounds).is_none() {
                return (event::Status::Ignored, None);
            }
            if self.seq_seq != state.seq_seq {
                *state = Self::State::default();
                state.seq_seq = self.seq_seq;
            }
            let resp = match event {
                Event::Keyboard(kbd_event) => {
                    let msg = match kbd_event {
                        iced::keyboard::Event::KeyPressed {
                            key_code: KeyCode::Backspace,
                            ..
                        } => {
                            state.cursor = state.cursor.saturating_sub(1);
                            Some(Msg::Remove(state.cursor))
                        }
                        iced::keyboard::Event::KeyPressed {
                            key_code: KeyCode::Left,
                            ..
                        } => {
                            state.cursor = state.cursor.saturating_sub(1);
                            Some(Msg::Tick)
                        }
                        iced::keyboard::Event::KeyPressed {
                            key_code: KeyCode::Right,
                            ..
                        } => {
                            state.cursor = (state.cursor + 1).min(self.seq.len());
                            Some(Msg::Tick)
                        }
                        iced::keyboard::Event::CharacterReceived(c) if !c.is_control() => {
                            let insert_idx = state.cursor;
                            state.cursor += 1;
                            Some(Msg::Insert(insert_idx, c))
                        }
                        _ => None,
                    };
                    (event::Status::Captured, msg)
                }
                _ => (event::Status::Ignored, Some(Msg::Tick)),
            };

            let k = 0.2;
            let h_spacing = 40.0;
            let v_spacing = 48.0;

            // Helper to get position of any node, including characters inside runs
            let get_node_pos = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                if let Some(pos) = nodes.get(id) {
                    return Some(*pos);
                }
                // Check if this ID is inside a run
                if let Some(hashseq::RunPosition::InRun { run_id, .. }) = self.seq.run_index.get(id)
                {
                    return nodes.get(run_id).copied();
                }
                None
            };

            // Helper to get the right edge of a node (for InsertAfter positioning)
            let get_node_right_edge = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                if let Some(run) = self.seq.runs.get(id) {
                    // For runs, return the right edge
                    if let Some(center) = nodes.get(id) {
                        let text_size = 24.0;
                        let char_width = text_size * 0.6;
                        let padding = 8.0;
                        let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                        return Some(Point {
                            x: center.x + width / 2.0,
                            y: center.y,
                        });
                    }
                }
                // For individual nodes or if inside a run, use center position
                get_node_pos(id, nodes)
            };

            // Helper to get the left edge of a node (for InsertBefore positioning)
            let get_node_left_edge = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                if let Some(run) = self.seq.runs.get(id) {
                    // For runs, return the left edge
                    if let Some(center) = nodes.get(id) {
                        let text_size = 24.0;
                        let char_width = text_size * 0.6;
                        let padding = 8.0;
                        let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                        return Some(Point {
                            x: center.x - width / 2.0,
                            y: center.y,
                        });
                    }
                }
                // For individual nodes or if inside a run, use center position
                get_node_pos(id, nodes)
            };

            let pos_in_set =
                |id: Id, set: BTreeSet<Id>, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                    let before_id = set.range(..id).next_back().cloned();
                    let after_id = set.range(id..).nth(1).cloned();
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

                // Process individual nodes
                for (id, node) in self.seq.individual_nodes.iter() {
                    let pos = *state.node_pos.entry(*id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });
                    let target_pos = match &node.op {
                        hashseq::Op::InsertRoot(_) => {
                            match pos_in_set(*id, self.seq.topo.roots().clone(), &state.node_pos) {
                                Some(p) => p,
                                None => pos,
                            }
                        }
                        hashseq::Op::InsertAfter(parent, _) => {
                            let w = 0.5;
                            if let Some(p) = get_node_right_edge(parent, &state.node_pos) {
                                let default_target = Point {
                                    x: p.x + h_spacing,
                                    y: p.y,
                                };
                                match pos_in_set(*id, self.seq.topo.after(*parent), &state.node_pos)
                                {
                                    None => default_target,
                                    Some(target) => Point {
                                        x: target.x * w + default_target.x * (1.0 - w),
                                        y: target.y * w + default_target.y * (1.0 - w),
                                    },
                                }
                            } else {
                                pos
                            }
                        }
                        hashseq::Op::InsertBefore(parent, _) => {
                            let w = 0.9;
                            if let Some(p) = get_node_left_edge(parent, &state.node_pos) {
                                let befores = self.seq.topo.before(*parent);
                                let default_target = Point {
                                    x: p.x - h_spacing,
                                    y: p.y + v_spacing * befores.len() as f32,
                                };
                                match pos_in_set(*id, befores, &state.node_pos) {
                                    None => default_target,
                                    Some(target) => Point {
                                        x: target.x * w + default_target.x * (1.0 - w),
                                        y: target.y * w + default_target.y * (1.0 - w),
                                    },
                                }
                            } else {
                                pos
                            }
                        }
                        hashseq::Op::Remove(targets) => {
                            assert!(!targets.is_empty());
                            let p: Vector = targets
                                .iter()
                                .filter_map(|t| get_node_pos(t, &state.node_pos))
                                .map(|p| Vector::new(p.x, p.y))
                                .reduce(|accum, p| accum + p)
                                .unwrap_or_default();

                            Point {
                                x: p.x,
                                y: p.y - 5.0,
                            }
                        }
                    };

                    let delta = Vector::<f32> {
                        x: target_pos.x - pos.x,
                        y: target_pos.y - pos.y,
                    };

                    let push = delta * k;
                    net_change += (push.x.powf(2.0) + push.y.powf(2.0)).sqrt();
                    let pos = state.node_pos.entry(*id).or_default();
                    pos.x += push.x;
                    pos.y += push.y;
                }

                // Process runs - position each run as a single entity
                for (run_id, run) in self.seq.runs.iter() {
                    let pos = *state.node_pos.entry(*run_id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });

                    let left_pos = get_node_left_edge(run_id, &state.node_pos).unwrap_or(pos);

                    // Determine target position based on run structure
                    let target_pos = {
                        // Has left dependencies
                        let parent = run.insert_after;
                        if let Some(p) = get_node_right_edge(&parent, &state.node_pos) {
                            Point {
                                x: p.x + h_spacing,
                                y: p.y,
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
                    let pos = state.node_pos.entry(*run_id).or_default();
                    pos.x += push.x;
                    pos.y += push.y;
                }

                //     // Collision detection for all nodes (individual nodes + run IDs)
                //     let mut all_node_ids: Vec<_> = self.seq.individual_nodes.keys().cloned().collect();
                //     all_node_ids.extend(self.seq.runs.keys().cloned());

                //     // Helper to get the radius/half-width of a node
                //     let get_node_radius = |id: &Id| -> f32 {
                //         if let Some(run) = self.seq.runs.get(id) {
                //             // For runs, use half the text width plus padding
                //             let text_size = 24.0;
                //             let char_width = text_size * 0.6;
                //             let padding = 8.0;
                //             let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                //             width / 2.0
                //         } else {
                //             // For individual nodes, use a small radius
                //             6.0
                //         }
                //     };

                //     for (i, a_id) in all_node_ids.iter().enumerate() {
                //         for b_id in all_node_ids.iter().skip(i + 1) {
                //             let a = state.node_pos[a_id];
                //             let b = state.node_pos[b_id];

                //             // Calculate minimum distance based on both node sizes
                //             let a_radius = get_node_radius(a_id);
                //             let b_radius = get_node_radius(b_id);
                //             let min_d = a_radius + b_radius + 4.0; // Add 4.0 for extra spacing

                //             let dx = b.x - a.x;
                //             let dy = b.y - a.y;
                //             let d_sq = (dx * dx + dy * dy).max(1.0);
                //             let min_d_sq = min_d * min_d;
                //             let rk = 0.01;
                //             if d_sq < min_d_sq {
                //                 let d = d_sq.sqrt();
                //                 let delta = min_d - d;
                //                 let nx = dx / d;
                //                 let ny = dy / d;
                //                 let rx = rand::random::<f32>() - 0.5;
                //                 let ry = rand::random::<f32>() - 0.5;
                //                 let fx = nx * delta * k + rx * rk;
                //                 let fy = ny * delta * k + ry * rk;

                //                 let f_net = (fx * fx + fy * fy).sqrt();
                //                 net_change += f_net * 2.0;

                //                 let a = state.node_pos.entry(*a_id).or_default();
                //                 a.x -= fx;
                //                 a.y -= fy;
                //                 let b = state.node_pos.entry(*b_id).or_default();
                //                 b.x += fx;
                //                 b.y += fy;
                //             }
                //         }
                //     }

                if i > 10 || net_change < 1e-4 {
                    break;
                }
            }

            if !state.node_pos.is_empty() {
                // Recenter things
                let mut avg_pos = Point::ORIGIN;
                for (_, pos) in state.node_pos.iter() {
                    avg_pos.x += pos.x;
                    avg_pos.y += pos.y;
                }
                avg_pos.x /= state.node_pos.len() as f32;
                avg_pos.y /= state.node_pos.len() as f32;
                let dx = bounds.width / 2.0 - avg_pos.x;
                let dy = bounds.height / 2.0 - avg_pos.y;

                for (_, pos) in state.node_pos.iter_mut() {
                    pos.x += dx;
                    pos.y += dy;
                }
            }
            println!("Converged in {i} iters");

            resp
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
            if self.seq_seq == state.seq_seq {
                let content =
                    self.state
                        .cache
                        .draw(renderer, bounds.size(), |frame: &mut Frame| {
                            // Helper to get position of any node, including characters inside runs
                            let get_node_pos = |id: &Id| -> Option<Point> {
                                if let Some(pos) = state.node_pos.get(id) {
                                    return Some(*pos);
                                }
                                // Check if this ID is inside a run
                                if let Some(hashseq::RunPosition::InRun { .. }) =
                                    self.seq.run_index.get(id)
                                {
                                    // TODO: determine position of char inside the run.
                                }
                                None
                            };

                            let string = String::from_iter(self.seq.iter());
                            let before_cursor =
                                String::from_iter(string.chars().take(state.cursor));
                            let after_cursor = String::from_iter(string.chars().skip(state.cursor));
                            let mut text = Text::from(format!("{before_cursor}|{after_cursor}"));
                            text.size = 32.0;
                            frame.fill_text(text);

                            for (id, span) in self.seq.topo.spans.iter() {
                                let Some(from) = get_node_pos(id) else {
                                    continue;
                                };
                                for after in span.after.iter() {
                                    let Some(to) = get_node_pos(after) else {
                                        continue;
                                    };
                                    frame.stroke(
                                        &Path::line(from, to),
                                        Stroke::default()
                                            .with_color(Color::from_rgb(0.0, 1.0, 0.0)),
                                    );
                                }
                            }
                            for (id, span) in self.seq.topo.spans.iter() {
                                let Some(from) = get_node_pos(id) else {
                                    continue;
                                };
                                for before in span.before.iter() {
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
                            for (id, pos) in state.node_pos.iter() {
                                // Check if this ID corresponds to a run
                                if let Some(run) = self.seq.runs.get(id) {
                                    // Render run as a single node with full text
                                    let text_size = 24.0;
                                    let char_width = text_size * 0.6; // Approximate character width
                                    let padding = 8.0;
                                    let width =
                                        run.run.chars().count() as f32 * char_width + padding * 2.0;
                                    let height = text_size + padding * 2.0;

                                    // Draw rectangle background
                                    let rect_pos = Point {
                                        x: pos.x - width / 2.0,
                                        y: pos.y - height / 2.0,
                                    };

                                    frame.fill(
                                        &Path::rectangle(rect_pos, Size::new(width, height)),
                                        Fill::from(Color::from_rgb(0.0, 0.5, 1.0)),
                                    );

                                    // Draw text centered
                                    let mut text = Text::from(run.run.clone());
                                    text.position = Point {
                                        x: pos.x
                                            - (run.run.chars().count() as f32 * char_width) / 2.0,
                                        y: pos.y - text_size / 2.0 + 2.0,
                                    };
                                    text.size = text_size;
                                    text.color = Color::WHITE;
                                    frame.fill_text(text);
                                } else if let Some(node) = self.seq.individual_nodes.get(id) {
                                    // Render individual node
                                    let r = 2.0;

                                    // Render dependencies for individual nodes
                                    if self.show_dependencies {
                                        for dep in node.extra_dependencies.iter() {
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

                                    match &node.op {
                                        hashseq::Op::InsertRoot(c)
                                        | hashseq::Op::InsertAfter(_, c)
                                        | hashseq::Op::InsertBefore(_, c) => {
                                            let mut char = Text::from(format!("{c}"));
                                            char.position = *pos;
                                            char.size = 24.0;
                                            char.position.y += 2.0;
                                            char.position.x -= char.size / 4.0;
                                            frame.fill_text(char);
                                        }
                                        hashseq::Op::Remove(targets) => {
                                            let x_r = 2.0;
                                            frame.stroke(
                                                &Path::line(
                                                    *pos + Vector::new(-x_r, -x_r),
                                                    *pos + Vector::new(x_r, x_r),
                                                ),
                                                Stroke::default().with_color(Color::BLACK),
                                            );
                                            frame.stroke(
                                                &Path::line(
                                                    *pos + Vector::new(x_r, -x_r),
                                                    *pos + Vector::new(-x_r, x_r),
                                                ),
                                                Stroke::default().with_color(Color::BLACK),
                                            );

                                            for target in targets.iter() {
                                                if let Some(to) = get_node_pos(target) {
                                                    frame.stroke(
                                                        &Path::line(*pos, to),
                                                        Stroke::default().with_color(Color::BLACK),
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    // Draw square node shape for individual elements
                                    frame.fill(
                                        &Path::rectangle(
                                            *pos - Vector::new(r * 0.5, r * 0.5),
                                            Size::new(r, r),
                                        ),
                                        Fill::from(Color::BLACK),
                                    );
                                }
                            }
                        });
                stack.push(content);
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
