window = global

// setTimeout=function(){}
// setInterval=function(){}

// XMLHttpRequest = function(){}
// XMLHttpRequest.prototype.send = function(){}
require("./bdms2")

t=[]

window.bdms.init(t)

function get_a_bogus(){
    // xhr=new XMLHttpRequest
    // xhr.send(null)
    return window.a_bogus

}
get_a_bogus()

console.log(window.bdms,get_a_bogus())
