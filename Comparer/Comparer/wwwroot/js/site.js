

$(window).scroll(function () {
    if ($(document).scrollTop() > $(window).height()/3 ) {
        $('.navbar').addClass('shrink');
        var color = '#487eb0';
        var rgbaCol = 'rgba(' + parseInt(color.slice(-6, -4), 16)
            + ',' + parseInt(color.slice(-4, -2), 16)
            + ',' + parseInt(color.slice(-2), 16)
            + ',0.97)';
        $('.navbar').css('background-color', rgbaCol);
        $('.navbar').css("transition", "1s");
    }
    else {
        $('.navbar').removeClass('shrink');
        $('.navbar').css("transition", "1s");
        $('.navbar').css("background-color", '#487eb0');
    }
});
