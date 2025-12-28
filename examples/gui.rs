use hashseq::HashSeq;
use iced::widget::{button, checkbox, column, row, text};
use iced::{Alignment, Element, Font, Length, Point, Rectangle, Sandbox, Settings, Theme};

pub fn main() -> iced::Result {
    Demo::run(Settings {
        antialiasing: true,
        default_font: Font::MONOSPACE,
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
                _ => (event::Status::Ignored, Some(Msg::Tick)),
            };

            let k = 0.2;
            let h_spacing = 50.0;
            let v_spacing = 48.0;

            // Helper to get position of any node, including characters inside runs
            let get_node_pos = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                if let Some(pos) = nodes.get(id) {
                    return Some(*pos);
                }
                // Check if this ID is inside a run
                if let Some(run_pos) = self.seq.run_index.get(id) {
                    // Get the run's first ID to find its position
                    if let Some(run) = self.seq.runs.get(&run_pos.run_id) {
                        return nodes.get(&run.first_id()).copied();
                    }
                }
                None
            };

            // Helper to get the right edge of a node (for InsertAfter positioning)
            let get_node_right_edge = |id: &Id, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                let text_size = 24.0;
                let char_width = text_size * 0.6;
                let padding = 8.0;

                // Check if id IS a run
                if let Some(run) = self.seq.runs.get(id)
                    && let Some(center) = nodes.get(id)
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x + width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is INSIDE a run
                if let Some(run_pos) = self.seq.run_index.get(id)
                    && let Some(run) = self.seq.runs.get(&run_pos.run_id)
                    && let Some(center) = nodes.get(&run.first_id())
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x + width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is a root node
                if self.seq.root_nodes.contains_key(id)
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
                let text_size = 24.0;
                let char_width = text_size * 0.6;
                let padding = 8.0;

                // Check if id IS a run
                if let Some(run) = self.seq.runs.get(id)
                    && let Some(center) = nodes.get(id)
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x - width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is INSIDE a run
                if let Some(run_pos) = self.seq.run_index.get(id)
                    && let Some(run) = self.seq.runs.get(&run_pos.run_id)
                    && let Some(center) = nodes.get(&run.first_id())
                {
                    let width = run.run.chars().count() as f32 * char_width + padding * 2.0;
                    return Some(Point {
                        x: center.x - width / 2.0,
                        y: center.y,
                    });
                }
                // Check if id is a root node
                if self.seq.root_nodes.contains_key(id)
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
                let roots: Vec<Id> = self.seq.root_nodes.keys().copied().collect();
                let num_roots = roots.len();
                let mut sorted_roots = roots.clone();
                sorted_roots.sort();

                for id in self.seq.root_nodes.keys() {
                    let pos = *state.node_pos.entry(*id).or_insert_with(|| Point {
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

                    let roots_vec: Vec<&Id> = self.seq.root_nodes.keys().collect();
                    let target_pos = match pos_in_set(*id, roots_vec, &state.node_pos) {
                        Some(p) => Point { x: p.x, y: p.y + lane_offset },
                        None => Point { x: pos.x, y: bounds.height / 2.0 + lane_offset },
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

                // Process before nodes - stratify concurrent before nodes into lanes
                // Before nodes should always be on a separate lane from their anchor
                for (id, before_node) in self.seq.before_nodes.iter() {
                    let pos = *state.node_pos.entry(*id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });
                    let parent = &before_node.anchor;
                    let target_pos = if let Some(p) = get_node_left_edge(parent, &state.node_pos) {
                        // Get all siblings (nodes before the same parent)
                        let siblings = self.seq.befores(parent);

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
                    let pos = state.node_pos.entry(*id).or_default();
                    pos.x += push.x;
                    pos.y += push.y;
                }

                // Process remove nodes
                for (id, remove_node) in self.seq.remove_nodes.iter() {
                    let pos = *state.node_pos.entry(*id).or_insert_with(|| Point {
                        x: rand::random::<f32>() * bounds.width,
                        y: rand::random::<f32>() * bounds.height,
                    });
                    let targets = &remove_node.nodes;
                    let target_pos = if !targets.is_empty() {
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
                    } else {
                        pos
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
                            // Check how many siblings this run has (concurrent branches from same parent)
                            let siblings = self.seq.afters(&parent);
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
                            let text_size = 24.0;
                            let char_width = text_size * 0.6;
                            let padding = 8.0;

                            // Helper to get center position of any node
                            let get_node_pos = |id: &Id| -> Option<Point> {
                                if let Some(pos) = state.node_pos.get(id) {
                                    return Some(*pos);
                                }
                                // Check if this ID is inside a run
                                if let Some(run_pos) = self.seq.run_index.get(id) {
                                    return state.node_pos.get(&run_pos.run_id).copied();
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
                            let before_cursor =
                                String::from_iter(string.chars().take(state.cursor));
                            let after_cursor = String::from_iter(string.chars().skip(state.cursor));
                            let mut text = Text::from(format!("{before_cursor}|{after_cursor}"));
                            text.size = 32.0;
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
                            for (id, pos) in state.node_pos.iter() {
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
