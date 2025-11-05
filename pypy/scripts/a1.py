class TextEditor:
    def __init__(self, cursor_pos = 0):
        self.result = []
        self.cursor_pos = cursor_pos
        self.undo_stack = []  # Undo stack
        self.redo_stack = []   # Redo stack
        
    def _save_state(self):
        self.undo_stack.append((self.result.copy(), self.cursor_pos))
        self.redo_stack.clear()

    def write(self, text: str) -> None:
        """
        Appends text to where the cursor is
        The cursor ends to the right of new text
        """
        self._save_state()
        self.result[self.cursor_pos:self.cursor_pos] = list(text)
        self.cursor_pos += len(text)
    

    def delete(self, k: int) -> None:
        """
        Deletes k characters to the left of the cursor
        """
        self._save_state()
        # heo|d
        k = min(self.cursor_pos, k)
        self.result = self.result[:self.cursor_pos-k] + self.result[self.cursor_pos:]
        # for i in range(self.cursor_pos-1, self.cursor_pos- k-1, -1):
        #     self.result.pop(i)
        self.cursor_pos -= k

    def cursor_left(self, k: int) -> None:
        """
        Moves the cursor k characters to the left
        """
        self._save_state()
        # heo|d
        k = min(self.cursor_pos, k)
        self.cursor_pos -= k

    def cursor_right(self, k: int) -> None:
        """
        Moves the cursor k characters to the right
        """
        # heo|d
        self._save_state()
        k = min(len(self.result) - self.cursor_pos, k)
        self.cursor_pos += k
    
    def undo(self, k):
        for _ in range(k):
            if self.undo_stack:
                prev_state = self.undo_stack.pop()
                self.redo_stack.append((self.result.copy(), self.cursor_pos))
                self.result, self.cursor_pos = prev_state

    def redo(self, k):
        for _ in range(k):
            if self.redo_stack:
                prev_state = self.redo_stack.pop()
                self.undo_stack.append((self.result.copy(), self.cursor_pos))
                self.result, self.cursor_pos = prev_state

    def display(self) -> str:
        """
        Returns a string in the form "xxxxx|xx"
        where '|' represents the cursor in the text
        """
        return ''.join(self.result[:self.cursor_pos]) + "|" + ''.join(self.result[self.cursor_pos:])


if __name__ == "__main__":
    editor = TextEditor()

    editor.write("hello")
    print(editor.display())  #hello|

    editor.write(" world!")
    print(editor.display())  #hello world!|

    editor.cursor_left(1)
    print(editor.display())  #hello world|!

    editor.delete(6)
    print(editor.display())  #hello|!

    editor.write(" dbt")
    print(editor.display())  #hello dbt|!

    editor.undo(2)
    print(editor.display())  #hello world|!

    editor.redo(1)
    print(editor.display())  #hello|!
