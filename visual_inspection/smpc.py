#!/usr/bin/env python3
"""
Interactive Visual Inspection Tool for Table Parsing

This tool provides a dual-panel interface for inspecting parsed tables:
- Left panel: Original PDF page
- Right panel: Parsed table from CSV

Keyboard controls:
- c: Mark as "correct"
- v: Mark as "almost" (almoSt)
- b: Mark as "bad"
- n: Mark as "none"
- Left/Right arrows: Navigate between instances
- q/Escape: Quit

Labels are automatically saved to a CSV file after each action.
(disclaimer: Made with Claude Sonnet 4.5)
"""

import csv
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import polars as pl
import pymupdf  # PyMuPDF
from PIL import Image, ImageTk


class TableInspectionTool:
    def __init__(self, root):
        self.root = root
        self.root.title("Table Parsing Visual Inspection Tool")
        self.root.geometry("1600x900")

        # Data storage
        self.pdf_files = []
        self.csv_files = []
        self.current_index = 0
        self.labels = []
        self.labels_file = None

        # Setup UI
        self.setup_ui()

        # Bind keyboard events
        self.bind_keyboard_events()

    def setup_ui(self):
        """Setup the user interface"""
        # Top menu bar
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Load Data Folder", command=self.load_data_folder)
        file_menu.add_command(label="Load Custom Files", command=self.load_custom_files)
        file_menu.add_separator()
        file_menu.add_command(label="Quit", command=self.quit_app)

        # Top control panel
        control_frame = ttk.Frame(self.root, padding="5")
        control_frame.pack(side=tk.TOP, fill=tk.X)

        # Navigation controls
        nav_frame = ttk.Frame(control_frame)
        nav_frame.pack(side=tk.LEFT, padx=5)
        ttk.Button(nav_frame, text="◄ Previous", command=self.previous_instance).pack(
            side=tk.LEFT, padx=2
        )

        self.index_label = ttk.Label(
            nav_frame, text="No data loaded", font=("Arial", 10, "bold")
        )
        self.index_label.pack(side=tk.LEFT, padx=10)

        ttk.Button(nav_frame, text="Next ►", command=self.next_instance).pack(
            side=tk.LEFT, padx=2
        )

        # Label buttons
        label_frame = ttk.LabelFrame(
            control_frame, text="Label (or use keyboard)", padding="5"
        )
        label_frame.pack(side=tk.LEFT, padx=20)

        ttk.Button(
            label_frame,
            text="Correct (C)",
            command=lambda: self.set_label("correct"),
            width=12,
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            label_frame,
            text="Almost (V)",
            command=lambda: self.set_label("almost"),
            width=12,
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            label_frame, text="Bad (B)", command=lambda: self.set_label("bad"), width=12
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            label_frame,
            text="None (N)",
            command=lambda: self.set_label("none"),
            width=12,
        ).pack(side=tk.LEFT, padx=2)

        # Current label display
        self.current_label_var = tk.StringVar(value="Not labeled")
        label_display = ttk.Label(
            control_frame,
            textvariable=self.current_label_var,
            font=("Arial", 11, "bold"),
            foreground="blue",
        )
        label_display.pack(side=tk.RIGHT, padx=20)

        # Main content area with two panels
        content_frame = ttk.Frame(self.root)
        content_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Left panel - PDF viewer
        left_panel = ttk.LabelFrame(content_frame, text="Original PDF", padding="5")
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=2)

        # PDF canvas with scrollbar
        pdf_scroll_frame = ttk.Frame(left_panel)
        pdf_scroll_frame.pack(fill=tk.BOTH, expand=True)

        pdf_scrollbar_y = ttk.Scrollbar(pdf_scroll_frame, orient=tk.VERTICAL)
        pdf_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)

        pdf_scrollbar_x = ttk.Scrollbar(pdf_scroll_frame, orient=tk.HORIZONTAL)
        pdf_scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)

        self.pdf_canvas = tk.Canvas(
            pdf_scroll_frame,
            bg="gray80",
            yscrollcommand=pdf_scrollbar_y.set,
            xscrollcommand=pdf_scrollbar_x.set,
        )
        self.pdf_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        pdf_scrollbar_y.config(command=self.pdf_canvas.yview)
        pdf_scrollbar_x.config(command=self.pdf_canvas.xview)

        # Right panel - CSV table viewer
        right_panel = ttk.LabelFrame(
            content_frame, text="Parsed Table (CSV)", padding="5"
        )
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=2)

        # Table display with scrollbars
        table_frame = ttk.Frame(right_panel)
        table_frame.pack(fill=tk.BOTH, expand=True)

        table_scrollbar_y = ttk.Scrollbar(table_frame, orient=tk.VERTICAL)
        table_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)

        table_scrollbar_x = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        table_scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)

        self.table_text = tk.Text(
            table_frame,
            wrap=tk.NONE,
            font=("Courier", 9),
            yscrollcommand=table_scrollbar_y.set,
            xscrollcommand=table_scrollbar_x.set,
        )
        self.table_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        table_scrollbar_y.config(command=self.table_text.yview)
        table_scrollbar_x.config(command=self.table_text.xview)

        # Status bar
        self.status_var = tk.StringVar(value="Ready. Please load data using File menu.")
        status_bar = ttk.Label(
            self.root, textvariable=self.status_var, relief=tk.SUNKEN
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def bind_keyboard_events(self):
        """Bind keyboard shortcuts"""
        self.root.bind("c", lambda e: self.set_label("correct"))
        self.root.bind("C", lambda e: self.set_label("correct"))
        self.root.bind("v", lambda e: self.set_label("almost"))
        self.root.bind("V", lambda e: self.set_label("almost"))
        self.root.bind("b", lambda e: self.set_label("bad"))
        self.root.bind("B", lambda e: self.set_label("bad"))
        self.root.bind("n", lambda e: self.set_label("none"))
        self.root.bind("N", lambda e: self.set_label("none"))
        self.root.bind("<Left>", lambda e: self.previous_instance())
        self.root.bind("<Right>", lambda e: self.next_instance())
        self.root.bind("q", lambda e: self.quit_app())
        self.root.bind("Q", lambda e: self.quit_app())
        self.root.bind("<Escape>", lambda e: self.quit_app())

    def load_data_folder(self):
        """Load PDFs and CSVs from a folder"""
        folder = filedialog.askdirectory(title="Select folder containing PDFs and CSVs")
        if not folder:
            return

        folder_path = Path(folder)

        # Find all PDF and CSV files
        pdf_files = sorted(folder_path.glob("*.pdf"))

        # Try to match them by name
        self.pdf_files = []
        self.csv_files = []

        for pdf_file in pdf_files:
            # Look for corresponding CSV (same name but .csv extension)
            csv_file = folder_path / f"{pdf_file.stem}.csv"
            if csv_file.exists():
                self.pdf_files.append(str(pdf_file))
                self.csv_files.append(str(csv_file))

        if not self.pdf_files:
            messagebox.showwarning(
                "No matches",
                "No matching PDF-CSV pairs found. Files should have the same name.",
            )
            return

        self.initialize_data()

    def load_custom_files(self):
        """Load custom PDF and CSV file lists"""
        messagebox.showinfo(
            "Load Custom Files",
            "First, select all PDF files, then select all corresponding CSV files in the same order.",
        )

        pdf_files = filedialog.askopenfilenames(
            title="Select PDF files", filetypes=[("PDF files", "*.pdf")]
        )
        if not pdf_files:
            return

        csv_files = filedialog.askopenfilenames(
            title="Select CSV files (in same order)", filetypes=[("CSV files", "*.csv")]
        )
        if not csv_files:
            return

        if len(pdf_files) != len(csv_files):
            messagebox.showerror("Error", "Number of PDF and CSV files must match!")
            return

        self.pdf_files = list(pdf_files)
        self.csv_files = list(csv_files)
        self.initialize_data()

    def initialize_data(self):
        """Initialize labels file and load first instance"""
        if not self.pdf_files:
            return

        # Create or load labels file
        base_dir = Path(self.pdf_files[0]).parent
        self.labels_file = base_dir / "labels.csv"

        # Initialize labels list
        self.labels = [""] * len(self.pdf_files)

        # Load existing labels if file exists
        if self.labels_file.exists():
            try:
                with open(self.labels_file, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        if i < len(self.labels):
                            self.labels[i] = row.get("label", "")
                self.status_var.set(
                    f"Loaded existing labels from {self.labels_file.name}"
                )
            except Exception as e:
                messagebox.showwarning(
                    "Warning", f"Could not load existing labels: {e}"
                )
        else:
            # Create new labels file
            self.save_all_labels()
            self.status_var.set(f"Created new labels file: {self.labels_file.name}")

        # Load first instance
        self.current_index = 0
        self.load_instance(0)

    def save_all_labels(self):
        """Save all labels to CSV file"""
        if not self.labels_file:
            return

        try:
            with open(self.labels_file, "w", newline="", encoding="utf-8") as f:
                fieldnames = ["index", "pdf_file", "csv_file", "label"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                for i in range(len(self.pdf_files)):
                    writer.writerow(
                        {
                            "index": i,
                            "pdf_file": Path(self.pdf_files[i]).name,
                            "csv_file": Path(self.csv_files[i]).name,
                            "label": self.labels[i],
                        }
                    )
            return True
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save labels: {e}")
            return False

    def load_instance(self, index):
        """Load and display a specific instance"""
        if not self.pdf_files or index < 0 or index >= len(self.pdf_files):
            return

        self.current_index = index

        # Update index label
        total = len(self.pdf_files)
        self.index_label.config(text=f"Instance {index + 1} / {total}")

        # Update current label display
        current_label = self.labels[index]
        if current_label:
            self.current_label_var.set(f"Current: {current_label.upper()}")
        else:
            self.current_label_var.set("Not labeled")

        # Load PDF
        self.load_pdf(self.pdf_files[index])

        # Load CSV
        self.load_csv(self.csv_files[index])

        # Update status
        pdf_name = Path(self.pdf_files[index]).name
        csv_name = Path(self.csv_files[index]).name
        self.status_var.set(f"Viewing: {pdf_name} | {csv_name}")

    def load_pdf(self, pdf_path):
        """Load and display PDF in left panel"""
        try:
            # Open PDF with PyMuPDF
            doc = pymupdf.open(pdf_path)

            # Render first page (can be extended to show multiple pages)
            page = doc[0]

            # Render at higher resolution for better quality
            zoom = 2.0
            mat = pymupdf.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat)

            # Convert to PIL Image
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

            # Convert to PhotoImage
            self.pdf_image = ImageTk.PhotoImage(img)

            # Display on canvas
            self.pdf_canvas.delete("all")
            self.pdf_canvas.create_image(0, 0, anchor=tk.NW, image=self.pdf_image)
            self.pdf_canvas.config(scrollregion=self.pdf_canvas.bbox(tk.ALL))

            doc.close()

        except Exception as e:
            self.pdf_canvas.delete("all")
            self.pdf_canvas.create_text(
                10,
                10,
                anchor=tk.NW,
                text=f"Error loading PDF:\n{e}",
                fill="red",
                font=("Arial", 10),
            )

    def load_csv(self, csv_path):
        """Load and display CSV table in right panel"""
        try:
            # Read CSV with polars
            df = pl.read_csv(csv_path)

            # Format as string table
            table_str = str(df)

            # Display in text widget
            self.table_text.delete(1.0, tk.END)
            self.table_text.insert(1.0, table_str)

        except Exception as e:
            self.table_text.delete(1.0, tk.END)
            self.table_text.insert(1.0, f"Error loading CSV:\n{e}")

    def set_label(self, label_value):
        """Set label for current instance and move to next"""
        if not self.pdf_files:
            messagebox.showwarning("No Data", "Please load data first!")
            return

        # Update label
        self.labels[self.current_index] = label_value

        # Save to file
        if not self.save_all_labels():
            return  # Don't advance if save failed

        # Update display
        self.current_label_var.set(f"Current: {label_value.upper()}")
        self.status_var.set(f"Labeled as '{label_value}' and saved. Moving to next...")

        # Auto-advance to next instance
        self.root.after(500, self.next_instance)  # Small delay for user feedback

    def next_instance(self):
        """Navigate to next instance"""
        if not self.pdf_files:
            return

        if self.current_index < len(self.pdf_files) - 1:
            self.load_instance(self.current_index + 1)
        else:
            self.status_var.set("Already at last instance")
            messagebox.showinfo("Complete", "You've reached the last instance!")

    def previous_instance(self):
        """Navigate to previous instance"""
        if not self.pdf_files:
            return

        if self.current_index > 0:
            self.load_instance(self.current_index - 1)
        else:
            self.status_var.set("Already at first instance")

    def quit_app(self):
        """Quit application"""
        if self.labels_file and any(self.labels):
            # Make sure everything is saved
            self.save_all_labels()

        self.root.quit()


def main():
    """Main entry point"""
    root = tk.Tk()
    TableInspectionTool(root)
    root.mainloop()


if __name__ == "__main__":
    main()
