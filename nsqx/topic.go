package nsqx

// Topic is a type that defines a topic, represented as a string.
type Topic string

// ToString converts a Topic instance to its string representation.
// This method allows instances of Topic to be used as strings.
// Returns: The string representation of the Topic instance.
func (t Topic) ToString() string {
	return string(t)
}
