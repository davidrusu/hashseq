use hashseq::HashSeq;
use iced::widget::{button, column, text};
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
    seq: HashSeq,
    bezier: bezier::State,
}

#[derive(Debug, Clone, Copy)]
enum Message {
    Clear,
    Insert(usize, char),
    Remove(usize),
    Tick,
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
        match message {
            Message::Clear => {
                self.bezier = bezier::State::default();
                self.seq = HashSeq::default();
                self.seq_seq += 1;
                self.bezier.request_redraw()
            }
            Message::Insert(idx, c) => {
                println!("Handled insert");
                self.seq.insert(idx, c);
                println!("Seq: {}", String::from_iter(self.seq.iter()));
                self.bezier.request_redraw();
            }
            Message::Remove(idx) => {
                self.seq.remove(idx);
                self.bezier.request_redraw()
            }
            Message::Tick => {
                self.bezier.request_redraw();
            }
        }
    }

    fn view(&self) -> Element<Message> {
        column![
            text("HashSeq Demo").width(Length::Shrink).size(36),
            self.bezier.view(self.seq_seq, &self.seq),
            button("Clear").padding(8).on_press(Message::Clear),
        ]
        .padding(20)
        .spacing(20)
        .align_items(Alignment::Center)
        .into()
    }
}

mod bezier {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use hashseq::Id;
    use iced::keyboard::KeyCode;
    use iced::widget::canvas::event::{self, Event};
    use iced::widget::canvas::{self, Canvas, Cursor, Fill, Frame, Geometry, Path, Stroke, Text};
    use iced::{mouse, Color, Size, Vector};

    #[derive(Default)]
    pub struct State {
        cache: canvas::Cache,
    }

    impl State {
        pub fn view<'a>(&'a self, seq_seq: usize, seq: &'a HashSeq) -> Element<'a, Message> {
            Canvas::new(HashSeqDemo {
                seq_seq,
                state: self,
                seq,
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
    }

    #[derive(Default)]
    struct ProgramState {
        seq_seq: usize,
        cursor: usize,
        node_pos: BTreeMap<Id, Point>,
    }

    impl<'a> canvas::Program<Message> for HashSeqDemo<'a> {
        type State = ProgramState;

        fn update(
            &self,
            state: &mut Self::State,
            event: Event,
            bounds: Rectangle,
            _cursor: Cursor,
        ) -> (event::Status, Option<Message>) {
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
                            Some(Message::Remove(state.cursor))
                        }
                        iced::keyboard::Event::KeyPressed {
                            key_code: KeyCode::Left,
                            ..
                        } => {
                            state.cursor = state.cursor.saturating_sub(1);
                            Some(Message::Tick)
                        }
                        iced::keyboard::Event::KeyPressed {
                            key_code: KeyCode::Right,
                            ..
                        } => {
                            state.cursor = (state.cursor + 1).min(self.seq.len());
                            Some(Message::Tick)
                        }
                        iced::keyboard::Event::CharacterReceived(c) if !c.is_control() => {
                            let insert_idx = state.cursor;
                            state.cursor += 1;
                            Some(Message::Insert(insert_idx, c))
                        }
                        _ => None,
                    };
                    (event::Status::Captured, msg)
                }
                _ => (event::Status::Ignored, Some(Message::Tick)),
            };

            let k = 0.1;
            let h_spacing = 10.0;
            let v_spacing = 48.0;

            let pos_in_set =
                |id: Id, set: BTreeSet<Id>, nodes: &BTreeMap<Id, Point>| -> Option<Point> {
                    let before_id = set.range(..id).rev().next().cloned();
                    let after_id = set.range(id..).nth(1).cloned();
                    let before_pos = before_id.and_then(|id| nodes.get(&id)).cloned();
                    let after_pos = after_id.and_then(|id| nodes.get(&id)).cloned();

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
                for (id, node) in self.seq.nodes.iter() {
                    let pos = state
                        .node_pos
                        .entry(*id)
                        .or_insert_with(|| Point {
                            x: rand::random::<f32>() * bounds.width,
                            y: rand::random::<f32>() * bounds.height,
                        })
                        .clone();
                    let target_pos = match node.op {
                        hashseq::Op::InsertRoot(_) => {
                            match pos_in_set(*id, self.seq.topo.roots.clone(), &state.node_pos) {
                                Some(p) => p,
                                None => pos,
                            }
                        }
                        hashseq::Op::InsertAfter(parent, _) => {
                            let w = 0.5;
                            if let Some(p) = state.node_pos.get(&parent) {
                                let default_target = Point {
                                    x: p.x + h_spacing,
                                    y: p.y,
                                };
                                match pos_in_set(*id, self.seq.topo.after(parent), &state.node_pos)
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
                            if let Some(p) = state.node_pos.get(&parent) {
                                let default_target = Point {
                                    x: p.x + h_spacing,
                                    y: p.y + v_spacing,
                                };
                                match pos_in_set(*id, self.seq.topo.before(parent), &state.node_pos)
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
                        hashseq::Op::Remove(target) => {
                            if let Some(p) = state.node_pos.get(&target) {
                                Point {
                                    x: p.x,
                                    y: p.y - 5.0,
                                }
                            } else {
                                pos
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

                for a_id in self.seq.nodes.keys() {
                    for b_id in self.seq.nodes.keys() {
                        if a_id == b_id {
                            continue;
                        }

                        let a = state.node_pos[a_id];
                        let b = state.node_pos[b_id];

                        // are they too close together?

                        let dx = b.x - a.x;
                        let dy = b.y - a.y;
                        let d_sq = (dx * dx + dy * dy).max(1.0);
                        let min_d_sq = 36.0;
                        let rk = 0.01;
                        if d_sq < min_d_sq {
                            let d = d_sq.sqrt();
                            let min_d = min_d_sq.sqrt();
                            let delta = min_d - d;
                            let nx = dx / d;
                            let ny = dy / d;
                            let rx = rand::random::<f32>() - 0.5;
                            let ry = rand::random::<f32>() - 0.5;
                            let fx = nx * delta * k + rx * rk;
                            let fy = ny * delta * k + ry * rk;

                            let f_net = (fx * fx + fy * fy).sqrt();
                            net_change += f_net * 2.0;

                            let a = state.node_pos.entry(*a_id).or_default();
                            a.x -= fx;
                            a.y -= fy;
                            let b = state.node_pos.entry(*b_id).or_default();
                            b.x += fx;
                            b.y += fy;
                        }
                    }
                }

                if net_change < 1.0 + (self.seq.nodes.len() * 10) as f32 {
                    break;
                }
            }

            if state.node_pos.len() > 0 {
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
            _theme: &Theme,
            bounds: Rectangle,
            _cursor: Cursor,
        ) -> Vec<Geometry> {
            let mut stack = Vec::new();
            if self.seq_seq == state.seq_seq {
                let content = self.state.cache.draw(bounds.size(), |frame: &mut Frame| {
                    let string = String::from_iter(self.seq.iter());
                    let before_cursor = String::from_iter(string.chars().take(state.cursor));
                    let after_cursor = String::from_iter(string.chars().skip(state.cursor));
                    frame.fill_text(Text::from(format!("{}|{}", before_cursor, after_cursor)));

                    for (id, afters) in self.seq.topo.after.iter() {
                        if !state.node_pos.contains_key(id) {
                            continue;
                        }
                        let from = state.node_pos[id].clone();
                        for after in afters.iter() {
                            if !state.node_pos.contains_key(after) {
                                continue;
                            }
                            let to = state.node_pos[after].clone();
                            frame.stroke(
                                &Path::line(from, to),
                                Stroke::default().with_color(Color::from_rgb(0.0, 1.0, 0.0)),
                            );
                        }
                    }
                    for (id, befores) in self.seq.topo.before.iter() {
                        if !state.node_pos.contains_key(id) {
                            continue;
                        }
                        let from = state.node_pos[id].clone();
                        for before in befores.iter() {
                            if !state.node_pos.contains_key(before) {
                                continue;
                            }
                            let to = state.node_pos[before].clone();
                            frame.stroke(
                                &Path::line(from, to),
                                Stroke::default().with_color(Color::from_rgb(1.0, 0.0, 0.0)),
                            );
                        }
                    }

                    for (id, pos) in state.node_pos.iter() {
                        let r = 2.0;

                        match &self.seq.nodes[id].op {
                            hashseq::Op::InsertRoot(c)
                            | hashseq::Op::InsertAfter(_, c)
                            | hashseq::Op::InsertBefore(_, c) => {
                                let mut char = Text::from(format!("{c}"));
                                char.position = *pos;
                                char.size = 12.0;
                                char.position.y += 2.0;
                                char.position.x -= char.size / 4.0;
                                frame.fill_text(char);
                            }
                            hashseq::Op::Remove(target) => {
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

                                let to = state.node_pos[target].clone();
                                frame.stroke(
                                    &Path::line(*pos, to),
                                    Stroke::default().with_color(Color::BLACK),
                                );
                            }
                        }

                        frame.fill(
                            &Path::rectangle(*pos - Vector::new(r * 0.5, r * 0.5), Size::new(r, r)),
                            Fill::from(Color::BLACK),
                        );
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
            cursor: Cursor,
        ) -> mouse::Interaction {
            if cursor.is_over(&bounds) {
                mouse::Interaction::Crosshair
            } else {
                mouse::Interaction::default()
            }
        }
    }
}
