/* Memory layout
[base_address] nb_options (assume read only so no conflicts, otherwise read it once in the first piece)
[base_address + 1] option_0
...
[base_address + 1 + i] option_i
...
[base_address + 1 + nb_options] last_option
 */


struct FindBestFit {
    options: Vec<u32>
}

impl FindBestFit {
    pub fn new(options: Vec<u32>) -> Self {
        Self{ options }
    }

    pub fn find_best_fit(&mut self, start_index: usize) -> (usize, u32) {
        let mut index_max = start_index;
        let mut max = self.options[start_index];

        for index in start_index..self.options.len() {
            let option = self.options[index];
            if option > max {
                index_max = index;
                max = option;
            }
        }

        for index in 0..self.options.len() {
            let option = self.options[index];
            if option > max {
                index_max = index;
                max = option;
            }
        }

        (index_max, max)
    }
}