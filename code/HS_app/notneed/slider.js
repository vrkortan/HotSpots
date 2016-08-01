/*from https://gist.github.com/dzenkovich/5813630*/

var Slider = function(options){
    var bar,
        slider,
        toggle,
        percent,
        that = this;

    function _init(options){
        _construct();

        if(options.container && options.container.appendChild){
            options.container.appendChild(bar);
        }
        else{
            document.getElementById(options.container).appendChild(bar);
        }

        if(options.value){
            that.value(options.value);
        }
    };

    function _construct(){
        bar = document.createElement('div');
        bar.className = 'slider-bar';
        slider = document.createElement('div');
        slider.className = 'slider-slider';
        bar.appendChild(slider);
        toggle = document.createElement('div');
        toggle.className = 'slider-toggle';
        slider.appendChild(toggle);

        bar.addEventListener('mousedown', _startSlide, false);
    };

    function _startSlide(e){
        var x = e.offsetX==undefined?e.layerX:e.offsetX;

        percent = (x / bar.offsetWidth).toFixed(2);
        slider.style.width = (percent * 100) + '%';

        document.addEventListener('mousemove', _moveSlide, false);
        document.addEventListener('mouseup', _stopSlide, false);

        _onChange();
    };

    function _moveSlide(e){
        if(e.target == bar){
            var x = e.offsetX==undefined?e.layerX:e.offsetX;

            percent = (x / bar.offsetWidth).toFixed(2);
            slider.style.width = (percent * 100) + '%';
            _onChange();
        }
    };

    function _stopSlide(e){
        document.removeEventListener('mousemove', _moveSlide, false);
        document.removeEventListener('mousemove', _stopSlide, false);
    };

    function _onChange(){
        if(typeof options.onChange == 'function'){
            options.onChange(percent);
        }
    };

    this.value = function(value){
        if(value == null){
            return percent;
        }
        else{
            percent = (value > 1 ? 1 : (value < 0 ? 0 : value));
            slider.style.width = (percent * 100) + '%';
            _onChange();
        }
    };

    _init(options);
};
